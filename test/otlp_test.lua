--- Проверки выгрузчика: старт и остановка, настройки, клиент, ресурс,
--- состояние. Отправка партий — в `ship_test.lua`.
---
--- Трасса, контекст, клиент и цикл — настоящие, из исходников; libcurl —
--- двойник транспорта `tnt-http`: по нему видно, что и с какими
--- заголовками ушло коллектору.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g, world = helper.group('tnt.trace.otlp')

--- Бросает ли старт с этими настройками и что именно.
---@param opts any
---@param expected string Текст без места
local function refused(opts, expected)
    local err, place = helper.raised(world.otlp.start, opts)

    t.assert_equals(err, place .. expected)
end

-- ── Константы и состояние до старта ──────────────────────────────────

g.test_defaults_are_those_of_the_contract = function()
    t.assert_equals(world.otlp.DEFAULT_ENDPOINT, 'http://127.0.0.1:4318')
    t.assert_equals(world.otlp.PATH, '/v1/traces')
    t.assert_equals(world.otlp.DEFAULT_BATCH, 512)
    t.assert_equals(world.otlp.DEFAULT_INTERVAL, 1)
    t.assert_equals(world.otlp.DEFAULT_TIMEOUT, 5)
    t.assert_equals(world.otlp.DEFAULT_RETRIES, 5)
    t.assert_equals(world.otlp.MAX_BACKOFF, 30)
    t.assert_equals(world.otlp.FIBER, 'trace_otlp')
    t.assert_equals(world.otlp.SERVICE_NAME, 'service.name')
    t.assert_equals(world.otlp.SERVICE_INSTANCE, 'service.instance.id')
    t.assert_equals(world.otlp.NOT_STARTED, 'выгрузка трасс не запущена')
    t.assert_equals(
        world.otlp.RIVAL,
        'очередь отрезков tnt-trace уже снимает tnt-sentry: '
            .. 'остановите его либо перезапустите Sentry без отрезков (spans = false)'
    )
    t.assert_equals(world.otlp.REFUSED, 'refused')
    t.assert_equals(world.otlp.EXHAUSTED, 'exhausted')
    t.assert_equals(world.otlp.TOO_LONG, 'too_long')
    t.assert_equals(world.otlp.encode, helper.module('tnt.trace.otlp.encode'))
    t.assert_equals(world.otlp.reply, helper.module('tnt.trace.otlp.reply'))
end

g.test_before_start_nothing_runs_and_flush_refuses = function()
    t.assert_equals(world.otlp.status(), {
        running = false,
        recording = false,
        sending = false,
        spans = { accepted = 0, rejected = 0, discarded = 0 },
        batches = { sent = 0, retried = 0, discarded = 0 },
    })
    t.assert_equals({ world.otlp.flush() }, { nil, 'выгрузка трасс не запущена' })

    -- Остановка до старта ничего не делает и ничего не пишет.
    world.otlp.stop()

    t.assert_equals(world.journal.records(), {})
end

-- ── Настройки старта ─────────────────────────────────────────────────

g.test_start_refuses_a_bad_shape_of_settings_on_the_caller_line = function()
    refused(
        helper.wrong('x'),
        'настройки выгрузки трасс — таблица, а не строка'
    )
    refused(
        {},
        'настройки выгрузки трасс.service_name — непустая строка, а не nil'
    )
    refused(
        { service_name = 'p', endpont = 'x' },
        'настройки выгрузки трасс: ключа «endpont» нет, есть batch, client, compression, endpoint, '
            .. 'headers, interval, resource, retries, service_name, timeout'
    )
    refused(
        { service_name = '', endpoint = 'http://x' },
        'настройки выгрузки трасс.service_name — непустая строка, а не пустая'
    )

    t.assert_equals(world.otlp.status().running, false)
end

g.test_start_refuses_numbers_out_of_range = function()
    refused(
        { service_name = 'p', batch = 0 },
        'настройки выгрузки трасс.batch — число больше 0, а не 0'
    )
    refused(
        { service_name = 'p', batch = 1.5 },
        'настройки выгрузки трасс.batch — целое число, а не 1.5'
    )
    refused(
        { service_name = 'p', interval = -1 },
        'настройки выгрузки трасс.interval — число больше 0, а не -1'
    )
    refused(
        { service_name = 'p', timeout = 0 },
        'настройки выгрузки трасс.timeout — число больше 0, а не 0'
    )
    refused(
        { service_name = 'p', retries = -1 },
        'настройки выгрузки трасс.retries — число не меньше 0, а не -1'
    )
end

g.test_start_refuses_a_bad_resource = function()
    refused(
        { service_name = 'p', resource = { team = { 'a' } } },
        'настройки выгрузки трасс.resource.team — строка или число или логическое значение, а не таблица'
    )
    refused(
        { service_name = 'p', resource = { 'panel' } },
        'настройки выгрузки трасс.resource: имя атрибута — непустая строка, а не число'
    )
    refused(
        { service_name = 'p', resource = { [''] = 'x' } },
        'настройки выгрузки трасс.resource: имя атрибута — непустая строка, а не пустая'
    )

    -- Имя службы в ресурсе разошлось бы с настройкой, которая его задаёт.
    refused(
        { service_name = 'p', resource = { ['service.name'] = 'q' } },
        'настройки выгрузки трасс.resource.service.name задаёт выгрузчик: имя службы — настройкой service_name'
    )
end

g.test_start_refuses_the_settings_of_the_client_the_exporter_sets_itself = function()
    local hints = {
        base_url = 'адрес — настройкой endpoint',
        headers = 'заголовки — настройкой headers',
        timeout = 'срок — настройкой timeout',
        propagate = 'отправка партии не рождает отрезков и не несёт заголовков контекста',
        retry = 'повторы партии ведёт выгрузчик',
        max_redirects = 'коллектор не переезжает',
    }

    for name, hint in pairs(hints) do
        refused(
            { service_name = 'p', client = { [name] = false } },
            ('настройки выгрузки трасс.client.%s задаёт выгрузчик: %s'):format(
                name,
                hint
            )
        )
    end

    t.assert_equals(world.otlp.status().running, false)
end

g.test_start_returns_a_refusal_when_the_client_cannot_be_built = function()
    t.assert_equals({ world.otlp.start({ service_name = 'p', endpoint = 'ftp://collector:4318' }) }, {
        nil,
        'клиент к коллектору не собран: адрес «ftp://collector:4318» не годится: коллектор слушает http или https',
    })
    t.assert_equals({ world.otlp.start({ service_name = 'p', endpoint = 'collector:4318' }) }, {
        nil,
        'клиент к коллектору не собран: адрес «collector:4318» не годится: коллектор слушает http или https',
    })
    t.assert_equals({ world.otlp.start({ service_name = 'p', client = { ssl_key = '/k' } }) }, {
        nil,
        'клиент к коллектору не собран: '
            .. 'ключ клиентского сертификата без сертификата не действует: задайте ssl_cert',
    })

    -- Ничего не запущено и не включено.
    t.assert_equals(world.otlp.status().running, false)
    t.assert_equals(world.trace.status().recording, false)
    t.assert_equals(world.http.hooks(), {})
end

g.test_the_refusal_does_not_show_the_credentials_of_the_address = function()
    local _, err = world.otlp.start({ service_name = 'p', endpoint = 'ftp://user:hunter2@collector:4318' })

    t.assert_equals(
        err,
        'клиент к коллектору не собран: адрес «ftp://user:[скрыто]@collector:4318» не годится: '
            .. 'коллектор слушает http или https'
    )
end

g.test_a_refused_start_leaves_a_running_export_untouched = function()
    helper.started(world.otlp, helper.accepting(1), { batch = 7 })

    t.assert_equals(world.otlp.start({ service_name = 'q', endpoint = 'ftp://x' }), nil)
    t.assert_equals(world.otlp.status().running, true)
    t.assert_equals(world.otlp.status().batch, 7)
    t.assert_equals(world.otlp.status().service_name, 'panel')
end

-- ── Старт: клиент, крюк, запись, цикл ────────────────────────────────

g.test_start_installs_the_hook_turns_recording_on_and_runs_the_loop = function()
    t.assert_equals(world.trace.status().recording, false)

    helper.started(world.otlp, helper.accepting(1))

    t.assert_equals(world.http.hooks(), { 'tnt.trace' })
    t.assert_equals(world.trace.status().hooks.http, true)
    t.assert_equals(world.trace.status().recording, true)
    t.assert_equals(world.otlp.status(), {
        running = true,
        recording = true,
        sending = false,
        endpoint = helper.ENDPOINT,
        service_name = 'panel',
        batch = 512,
        interval = 3600,
        retries = 5,
        compression = 'gzip',
        spans = { accepted = 0, rejected = 0, discarded = 0 },
        batches = { sent = 0, retried = 0, discarded = 0 },
    })

    -- Крюк поставлен до включения записи: предупреждения о забытом нет.
    t.assert_equals(world.journal.logged('крюк tnt-http не поставлен'), false)

    local started_record = world.journal.find('выгрузка трасс запущена')

    t.assert_not_equals(started_record, nil)
    ---@cast started_record TntTestingJournalRecord
    t.assert_equals(
        started_record.record.fields,
        { endpoint = helper.ENDPOINT, service_name = 'panel', batch = 512, compression = 'gzip' }
    )
end

g.test_start_hooks_the_loaded_contract_queues_and_only_them = function()
    local queues = { ['tnt.queue'] = helper.fake_queue(), ['tnt.event'] = helper.fake_queue() }
    local asked = {}

    world.otlp._set_source({
        monotonic = world.clock.monotonic,
        module = function(name)
            table.insert(asked, name)

            return queues[name]
        end,
    })

    helper.started(world.otlp, helper.accepting(1))

    table.sort(asked)

    t.assert_equals(asked, { 'tnt.amqp', 'tnt.event', 'tnt.kafka', 'tnt.queue' })
    t.assert_is(queues['tnt.queue'].get('tnt.trace'), world.trace.message_hooks.queue)
    t.assert_is(queues['tnt.event'].get('tnt.trace'), world.trace.message_hooks.event)
    t.assert_equals(world.http.hooks(), { 'tnt.trace' })
    t.assert_equals(
        world.trace.status().hooks,
        { http = true, queue = true, event = true, amqp = false, kafka = false }
    )
end

g.test_the_loaded_queues_are_asked_of_package_loaded = function()
    local kafka = helper.fake_queue()

    helper.with_loaded_queues({ ['tnt.kafka'] = kafka }, function()
        helper.started(world.otlp, helper.accepting(1))
    end)

    t.assert_is(kafka.get('tnt.trace'), world.trace.message_hooks.kafka)
    t.assert_equals(
        world.trace.status().hooks,
        { http = true, queue = false, event = false, amqp = false, kafka = true }
    )
end

g.test_the_address_and_the_status_hide_the_credentials = function()
    helper.started(world.otlp, helper.accepting(1), { endpoint = 'http://user:hunter2@collector:4318' })

    t.assert_equals(world.otlp.status().endpoint, 'http://user:[скрыто]@collector:4318')
    t.assert_equals(
        world.journal.find('выгрузка трасс запущена').record.fields.endpoint,
        'http://user:[скрыто]@collector:4318'
    )
end

g.test_the_status_shows_that_recording_was_turned_off_behind_the_exporter = function()
    helper.started(world.otlp, helper.accepting(1))

    -- `configure` берёт настройки трассы целиком: не названная запись
    -- выключается, и цикл выгружает пустоту.
    world.trace.configure({ sample_rate = 1 })

    t.assert_equals(world.otlp.status().running, true)
    t.assert_equals(world.otlp.status().recording, false)
end

g.test_the_exporter_client_is_built_with_the_settings_of_the_contract = function()
    local sent =
        helper.started(world.otlp, helper.accepting(1), { headers = { authorization = 'Bearer abc' }, timeout = 5 })

    helper.spans(world.trace, 1)
    world.otlp.flush()

    local call = helper.at(sent, 1)

    t.assert_equals(call.method, 'POST')
    t.assert_equals(call.url, helper.TRACES_URL)
    t.assert_equals(call.options.headers['content-type'], 'application/json')
    t.assert_equals(call.options.headers['authorization'], 'Bearer abc')
    t.assert_equals(call.options.headers['user-agent'], 'tnt-http')
    -- Срок ответа сверх срока соединения клиента: они складываются.
    t.assert_equals(call.options.timeout, 5 + 3)
    t.assert_equals(call.options.follow_location, false)
end

g.test_the_exporter_client_gets_exactly_the_settings_of_the_contract = function()
    -- POST клиент сам не повторяет ни при каком `attempts`, и по отправкам
    -- одна попытка не видна: настройки сверяются там, где клиент
    -- собирается. Модуль из исходников загружен на эту проверку и после
    -- неё выгружается — подмена `new` дальше неё не живёт.
    local http = world.http
    local build = http.new
    local asked = {}

    http.new = function(opts)
        table.insert(asked, table.deepcopy(opts))

        return build(opts)
    end

    helper.started(world.otlp, helper.accepting(1), {
        headers = { authorization = 'Bearer abc' },
        client = { user_agent = 'panel' },
    })

    t.assert_equals(asked, {
        {
            base_url = helper.ENDPOINT,
            headers = { authorization = 'Bearer abc' },
            timeout = 5,
            propagate = false,
            retry = { attempts = 1 },
            max_redirects = 0,
            user_agent = 'panel',
        },
    })
end

g.test_the_path_of_the_address_is_kept_as_a_prefix = function()
    local sent = helper.started(world.otlp, helper.accepting(1), { endpoint = helper.ENDPOINT .. '/otlp/' })

    helper.spans(world.trace, 1)
    world.otlp.flush()

    t.assert_equals(helper.at(sent, 1).url, helper.ENDPOINT .. '/otlp/v1/traces')
end

g.test_the_client_table_reaches_the_client_while_retries_and_redirects_stay_off = function()
    -- Одна попытка на 503 и без перехода на 302: повторы и переходы
    -- ведёт выгрузчик, а представление и срок соединения — вызывающего.
    local moved = helper.answer(302, { headers = { Location = 'http://x/' } })
    local sent = helper.started(world.otlp, { helper.answer(503), moved, helper.accepted() }, {
        batch = 1,
        client = { user_agent = 'panel', connect_timeout = 1 },
        timeout = 2,
    })

    helper.spans(world.trace, 2)

    local outcomes = world.otlp.flush()

    t.assert_equals(#sent, 1)
    t.assert_equals(outcomes[1].status, 503)
    t.assert_equals(helper.at(sent, 1).options.headers['user-agent'], 'panel')
    t.assert_equals(helper.at(sent, 1).options.timeout, 2 + 1)

    -- Повтор — ответ переходом: партия выброшена без перехода, следующая
    -- ушла тем же вызовом.
    world.clock.advance(1)
    outcomes = world.otlp.flush()

    t.assert_equals(#sent, 3)
    t.assert_equals(outcomes[1].status, 302)
    t.assert_equals(outcomes[2].ok, true)
    t.assert_equals(helper.at(sent, 3).url, helper.TRACES_URL)
    t.assert_equals(world.otlp.status().batches, { sent = 3, retried = 1, discarded = 1 })
end

g.test_the_exporter_does_not_trace_itself = function()
    local sent = helper.started(world.otlp, helper.accepting(2))
    local context = helper.module('tnt.context')

    helper.spans(world.trace, 1)

    context.run({ request_id = 'r-7' }, function()
        world.trace.within('handler', function()
            world.otlp.flush()

            -- Отправка партии внутри трассы не родила клиентского отрезка
            -- и не понесла заголовков контекста.
            t.assert_equals(world.trace.status().queued, 0)
            t.assert_equals(helper.at(sent, 1).options.headers['traceparent'], nil)
            t.assert_equals(helper.at(sent, 1).options.headers['x-request-id'], nil)
        end)
    end)

    -- А сам обработчик записан: запись включена и крюк на месте.
    t.assert_equals(world.trace.status().queued, 1)
end

-- ── Остановка и перенастройка ────────────────────────────────────────

g.test_stop_halts_the_loop_and_recording_but_keeps_the_hook = function()
    helper.started(world.otlp, helper.accepting(1))
    world.otlp.stop()

    t.assert_equals(world.otlp.status().running, false)
    t.assert_equals(world.trace.status().recording, false)
    t.assert_equals(world.http.hooks(), { 'tnt.trace' })
    t.assert_equals(world.journal.logged('выгрузка трасс остановлена'), true)

    -- Повторная остановка молчит.
    world.journal.forget()
    world.otlp.stop()

    t.assert_equals(world.journal.records(), {})
    t.assert_equals({ world.otlp.flush() }, { nil, 'выгрузка трасс не запущена' })
end

g.test_stop_does_not_send_what_is_queued = function()
    local sent = helper.started(world.otlp, helper.accepting(1))

    helper.spans(world.trace, 2)
    world.otlp.stop()

    t.assert_equals(#sent, 0)
    t.assert_equals(world.trace.status().queued, 2)
end

g.test_a_second_start_reconfigures_without_losing_the_pending_batch = function()
    local sent = helper.started(world.otlp, { helper.answer(503), helper.accepted() }, { batch = 1 })

    helper.spans(world.trace, 1)
    world.otlp.flush()

    t.assert_equals(world.otlp.status().pending.spans, 1)

    -- Новый клиент: то же, что ушло старым, уходит новым — двойник
    -- у транспорта общий, и второй запрос виден там же.
    t.assert_equals(world.otlp.start(helper.settings({ batch = 3, service_name = 'panel-2' })), true)

    local status = world.otlp.status()

    t.assert_equals(status.running, true)
    t.assert_equals(status.batch, 3)
    t.assert_equals(status.service_name, 'panel-2')
    t.assert_equals(status.pending.spans, 1)
    t.assert_equals(world.trace.status().recording, true)

    world.clock.advance(1)
    world.otlp.flush()

    t.assert_equals(#sent, 2)
    t.assert_equals(world.otlp.status().pending, nil)
    t.assert_equals(world.otlp.status().spans.accepted, 1)
end

-- ── Очередь снимает кто-то один ──────────────────────────────────────

--- Ставит двойник соседа: снимает ли очередь отрезков `tnt-sentry`.
---@param taking boolean
local function sentry_taking(taking)
    world.otlp._set_source({
        monotonic = world.clock.monotonic,
        rival = function()
            return taking
        end,
    })
end

g.test_start_is_refused_while_sentry_takes_the_queue = function()
    sentry_taking(true)

    t.assert_equals({ world.otlp.start(helper.settings()) }, { nil, world.otlp.RIVAL })

    -- Ни клиента, ни крюка, ни записи, ни цикла: очередь осталась Sentry.
    t.assert_equals(world.otlp.status().running, false)
    t.assert_equals(world.otlp.status().service_name, nil)
    t.assert_equals(world.trace.status().recording, false)
    t.assert_equals(world.http.hooks(), {})
    t.assert_equals(world.journal.logged('выгрузка трасс запущена'), false)

    -- Негодная форма — по-прежнему исключение: это ошибка вызывающего,
    -- и она видна раньше того, чем занята очередь.
    refused(
        {},
        'настройки выгрузки трасс.service_name — непустая строка, а не nil'
    )
end

g.test_a_start_refused_for_sentry_leaves_a_running_export_untouched = function()
    helper.started(world.otlp, helper.accepting(1), { batch = 7 })
    sentry_taking(true)

    t.assert_equals({ world.otlp.start(helper.settings({ batch = 9 })) }, { nil, world.otlp.RIVAL })
    t.assert_covers(world.otlp.status(), { running = true, recording = true, batch = 7 })
end

g.test_the_rival_is_the_loaded_sentry_that_takes_spans = function()
    local real = package.loaded['tnt.sentry']
    local shown = { running = true, recording = true, takes_spans = false }

    local ok, err = pcall(function()
        -- Незагруженный Sentry очереди не снимает.
        package.loaded['tnt.sentry'] = nil
        helper.started(world.otlp, helper.accepting(1))

        -- Идущий Sentry без отрезков — тоже, хоть запись трассы и включена:
        -- её включил сам выгрузчик, и перенастройка проходит.
        package.loaded['tnt.sentry'] = {
            status = function()
                return shown
            end,
        }
        t.assert_equals(world.otlp.start(helper.settings({ batch = 3 })), true)

        shown.takes_spans = true
        t.assert_equals({ world.otlp.start(helper.settings({ batch = 5 })) }, { nil, world.otlp.RIVAL })
        t.assert_equals(world.otlp.status().batch, 3)
    end)

    package.loaded['tnt.sentry'] = real

    t.assert_equals(ok, true, tostring(err))
end

-- ── Ресурс ───────────────────────────────────────────────────────────

--- Атрибуты ресурса первой отправленной партии.
---@param sent table[]
---@param index integer
---@return table[]
local function resource_of(sent, index)
    return helper.body_of(sent, index).resourceSpans[1].resource.attributes
end

g.test_the_resource_names_the_service_the_instance_and_what_was_given = function()
    world.otlp._set_source({
        instance = function()
            return 'panel-001-a'
        end,
    })

    local sent = helper.started(
        world.otlp,
        helper.accepting(1),
        { resource = { ['deployment.environment'] = 'stand', shard = 3 } }
    )

    helper.spans(world.trace, 1)
    world.otlp.flush()

    t.assert_equals(resource_of(sent, 1), {
        { key = 'deployment.environment', value = { stringValue = 'stand' } },
        { key = 'service.instance.id', value = { stringValue = 'panel-001-a' } },
        { key = 'service.name', value = { stringValue = 'panel' } },
        { key = 'shard', value = { intValue = '3' } },
    })
end

g.test_the_instance_given_in_the_resource_wins_over_the_name_of_the_node = function()
    world.otlp._set_source({
        instance = function()
            return 'panel-001-a'
        end,
    })

    local sent = helper.started(world.otlp, helper.accepting(1), { resource = { ['service.instance.id'] = 'blue' } })

    helper.spans(world.trace, 1)
    world.otlp.flush()

    t.assert_equals(resource_of(sent, 1), {
        { key = 'service.instance.id', value = { stringValue = 'blue' } },
        { key = 'service.name', value = { stringValue = 'panel' } },
    })
end

g.test_the_instance_name_comes_from_box_cfg_once_the_node_is_configured = function()
    world.otlp._set_source(nil)

    local real = box.cfg

    -- В процессе проверок узел не настроен: `box.cfg` — функция, имени нет.
    local sent = helper.started(world.otlp, helper.accepting(2))

    helper.spans(world.trace, 1)
    world.otlp.flush()

    t.assert_equals(resource_of(sent, 1), {
        { key = 'service.name', value = { stringValue = 'panel' } },
    })

    box.cfg = { instance_name = 'panel-001-a' }

    local ok, err = pcall(function()
        t.assert_equals(world.otlp.start(helper.settings()), true)
        helper.spans(world.trace, 1)
        world.otlp.flush()
    end)

    box.cfg = real

    t.assert_equals(ok, true, tostring(err))
    t.assert_equals(resource_of(sent, 2), {
        { key = 'service.instance.id', value = { stringValue = 'panel-001-a' } },
        { key = 'service.name', value = { stringValue = 'panel' } },
    })
end
