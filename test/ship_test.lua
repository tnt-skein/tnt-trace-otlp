--- Проверки отправки: партии, ответы коллектора, повторы, выброс,
--- одновременные отправки, цикл и будильник очереди.
---
--- Трасса, контекст, клиент и цикл — настоящие, из исходников; libcurl —
--- двойник транспорта `tnt-http`. Часы двойника одни у трассы
--- и у выгрузчика: срок повтора сверяется без настоящего ожидания.

local fiber = require('fiber')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local g, world = helper.group('tnt.trace.otlp.ship')

--- Запускает выгрузку с двойником коллектора и отдаёт, что ему ушло.
---@param script table[]
---@param overrides table|nil
---@return table sent
local function started(script, overrides)
    return helper.started(world.otlp, script, overrides)
end

--- Запускает выгрузку и снимает будильник очереди.
---
--- Для проверок ручных отправок, идущих одновременно: при партии в один
--- отрезок будильник будит цикл на каждом, и разбуженный такт встал бы
--- третьим в очередь отправок, а проверка сверяет порядок двух.
---@param script table[]
---@param overrides table|nil
---@return table sent
local function started_without_alarm(script, overrides)
    local sent = started(script, overrides)

    world.trace.on_filled(nil)

    return sent
end

--- Сколько отрезков в отправленной партии.
---@param sent table[]
---@param index integer
---@return integer
local function spans_in(sent, index)
    return #helper.body_of(sent, index).resourceSpans[1].scopeSpans[1].spans
end

--- Предупреждение по номеру; его отсутствие — ошибка самой проверки.
---@param index integer|nil С конца, если не задан
---@return table
local function warning(index)
    local warnings = helper.warnings(world.journal)

    return (assert(warnings[index or #warnings], 'предупреждения нет'))
end

-- ── Партия ───────────────────────────────────────────────────────────

g.test_flush_sends_the_queued_spans_as_one_otlp_batch = function()
    local sent = started(helper.accepting(1))
    local encode = helper.module('tnt.trace.otlp.encode')

    helper.spans(world.trace, 2)

    local records = world.trace.take(10)

    -- Обратно в очередь их не положить, поэтому партия сверяется
    -- с кодировщиком по тем же записям, снятым до отправки заново.
    helper.spans(world.trace, 2)

    t.assert_equals(world.otlp.flush(), { { spans = 2, ok = true, status = 200, rejected = 0 } })
    t.assert_equals(helper.body_of(sent, 1), encode.request(records, { ['service.name'] = 'panel' }))
    t.assert_equals(world.trace.status().queued, 0)
    t.assert_equals(world.otlp.status().spans, { accepted = 2, rejected = 0, discarded = 0 })
    t.assert_equals(world.otlp.status().batches, { sent = 1, retried = 0, discarded = 0 })
    t.assert_equals(helper.warnings(world.journal), {})
end

g.test_an_empty_queue_sends_nothing = function()
    local sent = started(helper.accepting(1))

    t.assert_equals(world.otlp.flush(), {})
    t.assert_equals(#sent, 0)
    t.assert_equals(world.otlp.status().batches.sent, 0)
end

g.test_full_batches_go_one_after_another_and_a_partial_one_ends_the_flush = function()
    local sent = started(helper.accepting(3), { batch = 2 })

    helper.spans(world.trace, 5)

    local outcomes = world.otlp.flush()

    t.assert_equals(#outcomes, 3)
    t.assert_equals(#sent, 3)
    t.assert_equals(spans_in(sent, 1), 2)
    t.assert_equals(spans_in(sent, 2), 2)
    t.assert_equals(spans_in(sent, 3), 1)
    t.assert_equals(helper.body_of(sent, 3).resourceSpans[1].scopeSpans[1].spans[1].name, 'span-5')
    t.assert_equals(world.otlp.status().spans.accepted, 5)
    t.assert_equals(world.otlp.status().batches.sent, 3)
end

g.test_exactly_full_batches_do_not_ask_the_queue_a_third_time = function()
    local sent = started(helper.accepting(2), { batch = 2 })

    helper.spans(world.trace, 4)

    t.assert_equals(#world.otlp.flush(), 2)
    t.assert_equals(#sent, 2)
end

g.test_a_partial_batch_ends_the_flush_even_if_spans_arrive_meanwhile = function()
    local arrived = false

    -- Слой клиента рождает отрезок посреди отправки: очередь уже не пуста,
    -- но неполная партия значит, что до отправки она была исчерпана.
    local function arriving(request, proceed)
        if not arrived then
            arrived = true
            helper.spans(world.trace, 1)
        end

        return proceed(request)
    end

    local sent = started(helper.accepting(2), { batch = 2, client = { layers = { arriving } } })

    helper.spans(world.trace, 1)

    t.assert_equals(#world.otlp.flush(), 1)
    t.assert_equals(#sent, 1)
    t.assert_equals(world.trace.status().queued, 1)
end

g.test_rejected_spans_of_a_partial_success_are_counted_and_said = function()
    local body = '{"partialSuccess":{"rejectedSpans":"1","errorMessage":"too old"}}'

    started({ helper.accepted({ body = body }) })
    helper.spans(world.trace, 3)

    t.assert_equals(world.otlp.flush(), { { spans = 3, ok = true, status = 200, rejected = 1 } })
    t.assert_equals(world.otlp.status().spans, { accepted = 2, rejected = 1, discarded = 0 })
    t.assert_equals(world.otlp.status().batches, { sent = 1, retried = 0, discarded = 0 })
    t.assert_equals(#helper.warnings(world.journal), 1)
    t.assert_equals(warning().module, 'tnt.trace.otlp')
    t.assert_equals(warning().record.message, 'коллектор отверг часть отрезков')
    t.assert_equals(warning().record.fields, { rejected = 1, err = 'too old' })

    -- Отвергнутое — не неудача выгрузки.
    t.assert_equals(world.otlp.status().last_error, nil)
end

g.test_more_rejected_than_sent_is_the_whole_batch_and_no_more = function()
    local body = '{"partialSuccess":{"rejectedSpans":"5"}}'

    local endless = '{"partialSuccess":{"rejectedSpans":"inf"}}'

    started({ helper.accepted({ body = body }), helper.accepted({ body = endless }) })
    helper.spans(world.trace, 3)

    t.assert_equals(world.otlp.flush(), { { spans = 3, ok = true, status = 200, rejected = 3 } })
    t.assert_equals(world.otlp.status().spans, { accepted = 0, rejected = 3, discarded = 0 })
    t.assert_equals(
        warning().record.fields,
        { rejected = 3, err = 'коллектор не назвал причины' }
    )

    helper.spans(world.trace, 2)

    t.assert_equals(world.otlp.flush()[1].rejected, 2)
    t.assert_equals(world.otlp.status().spans, { accepted = 0, rejected = 5, discarded = 0 })
end

-- ── Повторы ──────────────────────────────────────────────────────────

g.test_a_retriable_answer_postpones_the_batch_by_the_backoff = function()
    local sent = started({
        helper.answer(503, { reason = 'Busy', body = 'later' }),
        helper.accepted(),
        helper.accepted(),
    })

    helper.spans(world.trace, 2)

    t.assert_equals(world.otlp.flush(), {
        { spans = 2, ok = false, status = 503, retry_in = 1, err = 'ответ 503 Busy; later' },
    })

    local status = world.otlp.status()

    t.assert_equals(status.pending, { spans = 2, attempt = 2, retry_at = 1001 })
    t.assert_equals(status.batches, { sent = 1, retried = 1, discarded = 0 })
    t.assert_equals(status.spans, { accepted = 0, rejected = 0, discarded = 0 })
    t.assert_equals(status.last_error, 'ответ 503 Busy; later')
    t.assert_equals(#helper.warnings(world.journal), 1)
    t.assert_equals(warning().record.message, 'партия отрезков не принята, повтор')
    t.assert_equals(
        warning().record.fields,
        { spans = 2, attempt = 1, status = 503, err = 'ответ 503 Busy; later' }
    )

    -- До срока повтора ничего не уходит, и новые отрезки копятся в очереди.
    helper.spans(world.trace, 1)
    world.clock.advance(0.5)

    t.assert_equals(world.otlp.flush(), {})
    t.assert_equals(#sent, 1)
    t.assert_equals(world.trace.status().queued, 1)

    -- Ровно в срок — уходит та же партия, а за ней и накопленное.
    world.clock.advance(0.5)

    local outcomes = world.otlp.flush()

    t.assert_equals(#outcomes, 2)
    t.assert_equals(outcomes[1], { spans = 2, ok = true, status = 200, rejected = 0 })
    t.assert_equals(outcomes[2].spans, 1)
    t.assert_equals(helper.body_of(sent, 2), helper.body_of(sent, 1))
    t.assert_equals(world.otlp.status().pending, nil)
    t.assert_equals(world.otlp.status().spans.accepted, 3)
end

g.test_a_pending_batch_blocks_the_queue_and_a_failed_retry_postpones_again = function()
    local sent = started({ helper.answer(503), helper.answer(502) }, { batch = 1 })

    helper.spans(world.trace, 2)

    -- Полная партия не принята: вторая не берётся.
    t.assert_equals(#world.otlp.flush(), 1)
    t.assert_equals(world.trace.status().queued, 1)

    world.clock.advance(1)

    local outcomes = world.otlp.flush()

    t.assert_equals(#outcomes, 1)
    t.assert_equals(outcomes[1].status, 502)
    t.assert_equals(outcomes[1].retry_in, 2)
    t.assert_equals(#sent, 2)
    t.assert_equals(world.otlp.status().pending, { spans = 1, attempt = 3, retry_at = 1003 })
    t.assert_equals(world.trace.status().queued, 1)
end

--- Первый исход отправки одной партии под этот ответ коллектора.
---@param answer table
---@param overrides table|nil
---@return table
local function first_outcome(answer, overrides)
    started({ answer }, overrides)
    helper.spans(world.trace, 1)

    return world.otlp.flush()[1]
end

g.test_retry_after_is_the_lower_bound_of_the_wait = function()
    t.assert_equals(first_outcome(helper.answer(429, { headers = { ['Retry-After'] = '7' } })).retry_in, 7)
    t.assert_equals(world.otlp.status().pending.retry_at, 1007)
end

g.test_retry_after_does_not_shorten_the_backoff = function()
    local script = {}

    for _ = 1, 3 do
        table.insert(script, helper.answer(503, { headers = { ['Retry-After'] = '1.5' } }))
    end

    started(script)
    helper.spans(world.trace, 1)

    local delays = {}

    for _ = 1, 3 do
        table.insert(delays, world.otlp.flush()[1].retry_in)
        world.clock.advance(30)
    end

    -- Отступ 1, 2, 4; сервер просит полторы — больше только первого.
    t.assert_equals(delays, { 1.5, 2, 4 })
end

g.test_a_zero_retry_after_still_waits_the_backoff = function()
    t.assert_equals(first_outcome(helper.answer(503, { headers = { ['Retry-After'] = '0' } })).retry_in, 1)
end

g.test_retry_after_up_to_the_ceiling_is_waited = function()
    t.assert_equals(first_outcome(helper.answer(503, { headers = { ['Retry-After'] = '30' } })).retry_in, 30)
end

g.test_retry_after_over_the_ceiling_discards_the_batch_and_the_next_one_goes = function()
    local sent = started({
        helper.answer(429, { reason = 'Slow down', headers = { ['Retry-After'] = '31' } }),
        helper.accepted(),
    }, { batch = 1 })

    helper.spans(world.trace, 2)

    local outcomes = world.otlp.flush()

    t.assert_equals(outcomes[1], {
        spans = 1,
        ok = false,
        status = 429,
        err = 'ответ 429 Slow down; ',
        cause = 'too_long',
    })
    t.assert_equals(outcomes[2].ok, true)
    t.assert_equals(#sent, 2)
    t.assert_equals(world.otlp.status().pending, nil)
    t.assert_equals(world.otlp.status().batches, { sent = 2, retried = 0, discarded = 1 })
    t.assert_equals(warning().record.message, 'партия отрезков выброшена')
    t.assert_equals(warning().record.fields.cause, 'too_long')
end

g.test_no_answer_is_retried_with_the_reason_without_the_address = function()
    local outcome = first_outcome(helper.no_answer("Couldn't connect to server"), {
        endpoint = 'http://user:hunter2@collector:4318',
    })

    t.assert_equals(outcome.ok, false)
    t.assert_equals(outcome.status, nil)
    t.assert_equals(outcome.retry_in, 1)
    t.assert_equals(outcome.err, "сервер не ответил: Couldn't connect to server (код 595)")
    t.assert_equals(
        world.otlp.status().last_error,
        "сервер не ответил: Couldn't connect to server (код 595)"
    )
end

g.test_the_backoff_doubles_up_to_the_ceiling = function()
    local script = {}

    for _ = 1, 7 do
        table.insert(script, helper.answer(503))
    end

    started(script, { retries = 10 })
    helper.spans(world.trace, 1)

    local delays = {}

    for _ = 1, 7 do
        table.insert(delays, world.otlp.flush()[1].retry_in)
        world.clock.advance(30)
    end

    t.assert_equals(delays, { 1, 2, 4, 8, 16, 30, 30 })
end

g.test_the_batch_is_discarded_when_the_retries_are_spent = function()
    local sent = started({ helper.answer(503), helper.answer(503), helper.answer(503), helper.accepted() }, {
        retries = 2,
        batch = 1,
    })

    helper.spans(world.trace, 2)

    t.assert_equals(world.otlp.flush()[1].retry_in, 1)
    world.clock.advance(1)
    t.assert_equals(world.otlp.flush()[1].retry_in, 2)
    world.clock.advance(2)

    -- Третья неудача — повторы кончились: партия выброшена, а следующая
    -- уходит тем же вызовом.
    local outcomes = world.otlp.flush()

    t.assert_equals(
        outcomes[1],
        { spans = 1, ok = false, status = 503, err = 'ответ 503 Ok; ', cause = 'exhausted' }
    )
    t.assert_equals(outcomes[2].ok, true)
    t.assert_equals(#sent, 4)
    t.assert_equals(world.otlp.status().pending, nil)
    t.assert_equals(world.otlp.status().spans, { accepted = 1, rejected = 0, discarded = 1 })
    t.assert_equals(world.otlp.status().batches, { sent = 4, retried = 2, discarded = 1 })
    t.assert_equals(warning().record.message, 'партия отрезков выброшена')
    t.assert_equals(
        warning().record.fields,
        { spans = 1, attempt = 3, status = 503, err = 'ответ 503 Ok; ', cause = 'exhausted' }
    )
end

g.test_zero_retries_means_no_retry_at_all = function()
    local outcome = first_outcome(helper.answer(503), { retries = 0 })

    t.assert_equals(outcome.retry_in, nil)
    t.assert_equals(outcome.cause, 'exhausted')
    t.assert_equals(world.otlp.status().pending, nil)
    t.assert_equals(world.otlp.status().batches, { sent = 1, retried = 0, discarded = 1 })
end

-- ── Выброс ───────────────────────────────────────────────────────────

g.test_four_hundred_discards_the_batch_at_once_and_the_next_one_goes = function()
    local sent = started({ helper.answer(400, { reason = 'Bad', body = '{"code":3}' }), helper.accepted() }, {
        batch = 1,
    })

    helper.spans(world.trace, 2)

    local outcomes = world.otlp.flush()

    t.assert_equals(
        outcomes[1],
        { spans = 1, ok = false, status = 400, err = 'ответ 400 Bad; {"code":3}', cause = 'refused' }
    )
    t.assert_equals(outcomes[2], { spans = 1, ok = true, status = 200, rejected = 0 })
    t.assert_equals(#sent, 2)
    t.assert_equals(world.otlp.status().spans, { accepted = 1, rejected = 0, discarded = 1 })
    t.assert_equals(world.otlp.status().batches, { sent = 2, retried = 0, discarded = 1 })
    t.assert_equals(world.otlp.status().last_error, 'ответ 400 Bad; {"code":3}')
    t.assert_equals(#helper.warnings(world.journal), 1)
    t.assert_equals(warning().record.message, 'партия отрезков выброшена')
    t.assert_equals(
        warning().record.fields,
        { spans = 1, attempt = 1, status = 400, err = 'ответ 400 Bad; {"code":3}', cause = 'refused' }
    )
end

g.test_five_hundred_is_not_retried_even_with_retry_after = function()
    local outcome = first_outcome(helper.answer(500, { headers = { ['Retry-After'] = '1' } }))

    t.assert_equals(outcome.retry_in, nil)
    t.assert_equals(outcome.cause, 'refused')
    t.assert_equals(world.otlp.status().batches.discarded, 1)
end

-- ── Одновременные отправки ───────────────────────────────────────────

g.test_a_second_flush_waits_for_the_first_and_the_pending_batch_goes_once = function()
    local hold = helper.holding()
    local sent = started_without_alarm({ helper.answer(503), helper.accepted(), helper.accepted() }, {
        batch = 1,
        client = { layers = { hold.layer } },
    })

    helper.spans(world.trace, 2)
    world.otlp.flush()
    world.clock.advance(1)
    hold.close()

    -- Первая повторяет ждущую партию и стоит в пути.
    local first = helper.spawned(world.otlp.flush)

    t.assert_equals(hold.waiting, 1)
    t.assert_equals(world.otlp.status().sending, true)

    -- Вторая ждёт первую, а не шлёт ту же партию второй раз.
    local second = helper.spawned(world.otlp.flush)

    t.assert_equals(hold.waiting, 1)
    t.assert_equals(second:status(), 'suspended')

    hold.open()

    local first_ok, first_outcomes = first:join()
    local second_ok, second_outcomes = second:join()

    t.assert_equals(first_ok, true)
    t.assert_equals(#first_outcomes, 2)
    t.assert_equals(first_outcomes[1].ok, true)
    t.assert_equals(second_ok, true)
    t.assert_equals(second_outcomes, {})
    t.assert_equals(#sent, 3)
    t.assert_equals(world.otlp.status().sending, false)
    t.assert_equals(world.otlp.status().spans, { accepted = 2, rejected = 0, discarded = 0 })
    t.assert_equals(world.otlp.status().batches, { sent = 3, retried = 1, discarded = 0 })
end

g.test_a_flush_waiting_through_a_stop_refuses = function()
    local hold = helper.holding()
    local sent = started_without_alarm(helper.accepting(2), { batch = 1, client = { layers = { hold.layer } } })

    helper.spans(world.trace, 2)
    hold.close()

    local first = helper.spawned(world.otlp.flush)
    local second = helper.spawned(world.otlp.flush)

    -- Остановка посреди отправки: партия в пути доходит, следующей
    -- первая не берёт, а ждавшая вторая узнаёт, что выгрузки больше нет.
    world.otlp.stop()
    hold.open()

    local _, first_outcomes = first:join()

    t.assert_equals(first_outcomes, { { spans = 1, ok = true, status = 200, rejected = 0 } })
    t.assert_equals({ second:join() }, { true, nil, 'выгрузка трасс не запущена' })
    t.assert_equals(#sent, 1)
    t.assert_equals(world.trace.status().queued, 1)
    t.assert_equals(world.journal.logged('такт выгрузки трасс не отработал'), false)
end

g.test_a_restart_in_the_middle_of_a_flush_sends_the_rest_with_the_new_client = function()
    local hold = helper.holding()
    local sent = started(helper.accepting(2), {
        batch = 1,
        headers = { ['x-probe'] = 'old' },
        client = { layers = { hold.layer } },
    })

    helper.spans(world.trace, 2)
    hold.close()

    local first = helper.spawned(world.otlp.flush)

    -- Первый такт нового цикла идёт сразу и ждёт идущую отправку.
    t.assert_equals(world.otlp.start(helper.settings({ batch = 1, headers = { ['x-probe'] = 'new' } })), true)
    t.assert_equals(#sent, 0)

    hold.open()

    local _, first_outcomes = first:join()

    -- Такт нового цикла идёт своим файбером: ему нужен оборот.
    fiber.yield()

    t.assert_equals(#first_outcomes, 1)
    t.assert_equals(#sent, 2)
    t.assert_equals(helper.at(sent, 1).options.headers['x-probe'], 'old')
    t.assert_equals(helper.at(sent, 2).options.headers['x-probe'], 'new')
    t.assert_equals(world.otlp.status().spans.accepted, 2)
end

-- ── Цикл ─────────────────────────────────────────────────────────────

g.test_the_loop_sends_what_is_queued_on_its_own = function()
    local sent = started(helper.accepting(1), { interval = 0.01 })

    helper.spans(world.trace, 1)

    -- Такт настоящий: цикл идёт в своём файбере.
    fiber.sleep(0.1)

    t.assert_equals(#sent, 1)
    t.assert_equals(world.otlp.status().spans.accepted, 1)
end

g.test_the_first_tick_at_start_sends_what_was_queued_before = function()
    world.trace.record(true)
    helper.spans(world.trace, 1)

    local sent = started(helper.accepting(1))

    t.assert_equals(#sent, 1)
end

g.test_a_tick_that_raises_is_reported_and_the_loop_lives_on = function()
    local sent = started({ helper.answer(503), helper.accepted() })

    helper.spans(world.trace, 1)
    world.otlp.flush()

    -- Часы ломаются: ждущая партия читает их на каждом такте.
    local broken = true

    world.otlp._set_source({
        monotonic = function()
            if broken then
                error('часы сломаны', 0)
            end

            return world.clock.monotonic() + 1
        end,
    })

    t.assert_equals(world.otlp.start(helper.settings({ interval = 0.01 })), true)
    fiber.sleep(0.05)

    local found = world.journal.find('такт выгрузки трасс не отработал')

    t.assert_not_equals(found, nil)
    ---@cast found TntTestingJournalRecord
    -- Брошенное доходит до журнала как есть, без приписки места.
    t.assert_equals(found.record.fields.err, 'часы сломаны')
    t.assert_equals(world.otlp.status().running, true)
    t.assert_equals(world.otlp.status().sending, false)

    -- Сорвавшаяся отправка не держит следующую: часы починены — партия ушла.
    broken = false
    fiber.sleep(0.05)

    t.assert_equals(#sent, 2)
    t.assert_equals(world.otlp.status().pending, nil)
end

-- ── Будильник очереди ────────────────────────────────────────────────

g.test_a_gathered_batch_wakes_the_loop_and_a_partial_one_waits_for_the_tick = function()
    -- Такт — час: до него ушло бы только то, что будит будильник.
    local sent = started(helper.accepting(1), { batch = 2 })

    helper.spans(world.trace, 1)
    fiber.sleep(0.01)

    t.assert_equals(#sent, 0)

    -- Будильник не уступает: закрывший отрезок не ждёт выгрузки,
    -- и партия уходит, когда он сам уступит управление.
    helper.spans(world.trace, 1)

    t.assert_equals(#sent, 0)

    fiber.sleep(0.01)

    t.assert_equals(#sent, 1)
    t.assert_equals(spans_in(sent, 1), 2)
    t.assert_equals(world.trace.status().queued, 0)
end

--- Отрезков во всплеске: больше двух потолков очереди (2048).
local BURST = 5000

--- Всплеск: 5000 отрезков за 0,1 с — пятьдесят кусков по сотне, между
--- кусками сон по 2 мс.
---
--- Отрезки закрываются в файбере проверки, а выгрузка идёт в своём:
--- так под нагрузкой отрезки рождают запросы, а вывозит их цикл.
local function burst()
    for _ = 1, BURST / 100 do
        helper.spans(world.trace, 100)
        fiber.sleep(0.002)
    end
end

g.test_a_burst_over_the_capacity_goes_out_whole_when_the_collector_keeps_up = function()
    -- Такт — час: всплеск за такт больше потолка, и вывезти его без
    -- потерь успевает только будильник очереди.
    local sent = started(helper.accepting(100))

    burst()

    t.assert_equals(world.trace.status().dropped, 0)
    t.assert_ge(#sent, 10)

    -- Хвост меньше партии ждёт такта; досылает его ручной `flush`.
    world.otlp.flush()

    t.assert_equals(world.otlp.status().spans, { accepted = BURST, rejected = 0, discarded = 0 })
    t.assert_equals(world.trace.status().queued, 0)
end

g.test_without_the_alarm_a_burst_over_the_capacity_loses_the_excess = function()
    local sent = started(helper.accepting(100))

    -- Отрицательный контроль: будильник снят — очередь копится до такта,
    -- и всё сверх потолка теряется при свободном коллекторе.
    world.trace.on_filled(nil)
    burst()

    t.assert_equals(#sent, 0)
    t.assert_equals(world.trace.status().dropped, BURST - 2048)

    world.otlp.flush()

    t.assert_equals(world.otlp.status().spans.accepted, 2048)
end
