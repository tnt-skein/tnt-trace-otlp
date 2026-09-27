--- Проверки приговора ответу коллектора: таблицей векторов по кодам OTLP.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.trace.otlp.reply')

---@type any
local reply

---@type any
local failure_of

---@type any
local response_of

g.before_each(function()
    reply = helper.load('tnt.trace.otlp.reply')
    failure_of = helper.module('tnt.http.failure')
    response_of = helper.module('tnt.http.response')
end)

g.after_each(function()
    helper.forget()
end)

--- Ответ коллектора в виде, в каком его отдаёт клиент.
---@param status integer
---@param overrides table|nil
---@return table
local function answer(status, overrides)
    return response_of.new(helper.answer(status, overrides), { method = 'POST', url = helper.TRACES_URL })
end

g.test_the_verdicts_and_the_retriable_codes_are_those_of_otlp = function()
    t.assert_equals(reply.ACCEPTED, 'accepted')
    t.assert_equals(reply.RETRY, 'retry')
    t.assert_equals(reply.DISCARD, 'discard')
    t.assert_equals(reply.RETRIABLE, { [429] = true, [502] = true, [503] = true, [504] = true })
    t.assert_equals(reply.NO_REASON, 'коллектор не назвал причины')
end

-- ── Принята ──────────────────────────────────────────────────────────

g.test_two_hundred_with_an_empty_partial_success_is_accepted_whole = function()
    t.assert_equals(reply.judge(answer(200, { body = '{"partialSuccess":{}}' })), {
        verdict = 'accepted',
        status = 200,
        rejected = 0,
    })
end

g.test_any_two_xx_is_accepted_even_with_an_empty_or_odd_body = function()
    t.assert_equals(reply.judge(answer(200, { body = '' })).verdict, 'accepted')
    t.assert_equals(reply.judge(answer(200, { body = 'not json' })).verdict, 'accepted')
    t.assert_equals(reply.judge(answer(200, { body = '[]' })).verdict, 'accepted')
    t.assert_equals(reply.judge(answer(200, { body = '{"partialSuccess":5}' })).rejected, 0)
    t.assert_equals(reply.judge(answer(204, { body = '' })), { verdict = 'accepted', status = 204, rejected = 0 })
    t.assert_equals(reply.judge(answer(299)).verdict, 'accepted')
end

g.test_rejected_spans_are_counted_from_partial_success_with_the_reason = function()
    local body = '{"partialSuccess":{"rejectedSpans":"3","errorMessage":"too old"}}'

    t.assert_equals(reply.judge(answer(200, { body = body })), {
        verdict = 'accepted',
        status = 200,
        rejected = 3,
        reason = 'too old',
    })

    -- Число, а не строка — так тоже читается.
    t.assert_equals(reply.judge(answer(200, { body = '{"partialSuccess":{"rejectedSpans":2}}' })), {
        verdict = 'accepted',
        status = 200,
        rejected = 2,
        reason = 'коллектор не назвал причины',
    })
end

--- Сколько отвергнутых насчитал приговор по этому полю `rejectedSpans`.
---@param field string Поле JSON как есть
---@return integer
local function rejected_by(field)
    return reply.judge(answer(200, { body = '{"partialSuccess":{"rejectedSpans":' .. field .. '}}' })).rejected
end

g.test_a_zero_or_negative_or_unreadable_rejected_count_is_none = function()
    t.assert_equals(
        reply.judge(answer(200, { body = '{"partialSuccess":{"rejectedSpans":"0","errorMessage":"x"}}' })),
        {
            verdict = 'accepted',
            status = 200,
            rejected = 0,
        }
    )
    t.assert_equals(rejected_by('"-1"'), 0)
    t.assert_equals(rejected_by('"many"'), 0)
    t.assert_equals(rejected_by('null'), 0)
    t.assert_equals(rejected_by('true'), 0)
end

g.test_only_a_whole_rejected_count_is_taken = function()
    -- Дробь и NaN коллектор в виду не имел: счётчик отрезков — целый.
    t.assert_equals(rejected_by('"1.5"'), 0)
    t.assert_equals(rejected_by('0.5'), 0)
    t.assert_equals(rejected_by('"nan"'), 0)
    t.assert_equals(rejected_by('"1"'), 1)
    t.assert_equals(rejected_by('"1e3"'), 1000)

    -- Бесконечность целая: срезает её до партии выгрузчик, который партию знает.
    t.assert_equals(rejected_by('"inf"'), math.huge)
end

g.test_an_empty_error_message_is_replaced_by_a_default_one = function()
    local verdict = reply.judge(answer(200, { body = '{"partialSuccess":{"rejectedSpans":1,"errorMessage":""}}' }))

    t.assert_equals(verdict.reason, 'коллектор не назвал причины')
end

-- ── Повторить ────────────────────────────────────────────────────────

g.test_the_four_retriable_codes_ask_for_a_retry_with_the_reason = function()
    for _, status in ipairs({ 429, 502, 503, 504 }) do
        local verdict = reply.judge(answer(status, { reason = 'Busy', body = 'later' }))

        t.assert_equals(verdict, {
            verdict = 'retry',
            status = status,
            rejected = 0,
            reason = ('ответ %d Busy; later'):format(status),
        }, tostring(status))
    end
end

g.test_retry_after_is_taken_only_as_a_non_negative_number_of_seconds = function()
    t.assert_equals(reply.judge(answer(429, { headers = { ['Retry-After'] = '7' } })).delay, 7)
    t.assert_equals(reply.judge(answer(503, { headers = { ['retry-after'] = '0' } })).delay, 0)
    t.assert_equals(reply.judge(answer(503, { headers = { ['retry-after'] = '1.5' } })).delay, 1.5)
    t.assert_equals(reply.judge(answer(503, { headers = { ['retry-after'] = '-1' } })).delay, nil)
    t.assert_equals(
        reply.judge(answer(503, { headers = { ['retry-after'] = 'Wed, 21 Oct 2015 07:28:00 GMT' } })).delay,
        nil
    )
    t.assert_equals(reply.judge(answer(503)).delay, nil)
end

g.test_no_answer_is_a_retry_with_the_reason_without_the_address = function()
    local failure = failure_of.on(
        'unreachable',
        { method = 'POST', url = 'http://user:hunter2@collector/v1/traces' },
        "Couldn't connect"
    )

    t.assert_equals(reply.judge(nil, failure), { verdict = 'retry', rejected = 0, reason = "Couldn't connect" })
end

g.test_a_failure_without_a_short_reason_is_printed_whole = function()
    local failure = failure_of.new('refused', 'крюк отменил отправку')

    t.assert_equals(reply.judge(nil, failure).reason, 'крюк отменил отправку')
end

-- ── Выбросить ────────────────────────────────────────────────────────

g.test_four_hundred_and_the_other_codes_discard_the_batch = function()
    for _, status in ipairs({ 400, 401, 403, 404, 413, 415, 500, 501, 505 }) do
        local verdict = reply.judge(answer(status, { reason = 'Bad', body = '{"code":3}' }))

        t.assert_equals(verdict, {
            verdict = 'discard',
            status = status,
            rejected = 0,
            reason = ('ответ %d Bad; {"code":3}'):format(status),
        }, tostring(status))
    end
end

g.test_the_reason_keeps_only_the_beginning_of_a_long_body = function()
    local verdict = reply.judge(answer(400, { reason = 'Bad', body = ('x'):rep(1000) }))

    t.assert_equals(#verdict.reason < 400, true)
    t.assert_equals(verdict.reason:sub(-3), '…')
end

g.test_a_redirect_is_a_discard_because_the_exporter_does_not_follow = function()
    t.assert_equals(reply.judge(answer(302, { headers = { Location = 'http://elsewhere/' } })).verdict, 'discard')
end
