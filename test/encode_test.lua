--- Проверки кодировщика OTLP JSON: эталонными таблицами и телом,
--- разобранным обратно из JSON.
---
--- Три правила ProtoJSON, на которых коллектор строг, — опознаватели
--- шестнадцатеричной строкой, время десятичной строкой, род и статус
--- целыми, — сверяются дословно; тайны в атрибутах вырезаются.

local json = require('json')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.trace.otlp.encode')

---@type any
local encode

g.before_each(function()
    encode = helper.load('tnt.trace.otlp.encode')
end)

g.after_each(function()
    helper.forget()
end)

-- ── Константы ────────────────────────────────────────────────────────

g.test_the_enumerations_are_those_of_otlp = function()
    t.assert_equals(encode.SCOPE, 'tnt-trace')
    t.assert_equals(encode.KIND, { internal = 1, server = 2, client = 3, producer = 4, consumer = 5 })
    t.assert_equals(encode.UNSPECIFIED_KIND, 0)
    t.assert_equals(encode.ERROR_CODE, 2)
    t.assert_equals(encode.UNSET, 'unset')
end

-- ── Значения атрибутов ───────────────────────────────────────────────

g.test_scalars_become_any_values_by_their_type = function()
    t.assert_equals(encode.value('space', 'customers'), { stringValue = 'customers' })
    t.assert_equals(encode.value('cached', true), { boolValue = true })
    t.assert_equals(encode.value('cached', false), { boolValue = false })
    t.assert_equals(encode.value('rows', 7), { intValue = '7' })
    t.assert_equals(encode.value('rows', -7), { intValue = '-7' })
    t.assert_equals(encode.value('rows', 0), { intValue = '0' })
    t.assert_equals(encode.value('ratio', 0.5), { doubleValue = 0.5 })
    t.assert_equals(encode.value('ratio', -2.25), { doubleValue = -2.25 })
end

g.test_an_integer_at_the_edge_of_int64_is_an_int_and_beyond_it_a_double = function()
    -- `%d` насытил бы число за границей до предела int64, и атрибут врал бы.
    t.assert_equals(encode.value('big', 2 ^ 62), { intValue = '4611686018427387904' })
    t.assert_equals(encode.value('big', -(2 ^ 62)), { intValue = '-4611686018427387904' })
    t.assert_equals(encode.value('big', 2 ^ 63), { doubleValue = 2 ^ 63 })
    t.assert_equals(encode.value('big', -(2 ^ 63)), { doubleValue = -(2 ^ 63) })
    t.assert_equals(encode.value('big', 1e30), { doubleValue = 1e30 })
end

g.test_nan_and_infinities_go_as_strings_because_json_has_no_such_numbers = function()
    t.assert_equals(encode.value('x', 0 / 0), { stringValue = 'nan' })
    t.assert_equals(encode.value('x', 1 / 0), { stringValue = 'inf' })
    t.assert_equals(encode.value('x', -1 / 0), { stringValue = '-inf' })
end

g.test_what_is_not_a_scalar_is_printed = function()
    t.assert_equals(encode.value('x', nil), { stringValue = 'nil' })
    t.assert_equals(encode.value('x', 5LL), { stringValue = '5LL' })
end

g.test_a_secret_is_hidden_by_its_name_and_inside_a_string = function()
    t.assert_equals(encode.value('authorization', 'Bearer abc'), { stringValue = '[скрыто]' })
    t.assert_equals(encode.value('db.password', 42), { stringValue = '[скрыто]' })
    t.assert_equals(
        encode.value('url.full', 'https://ivan:hunter2@api.example.org/x'),
        { stringValue = 'https://ivan:[скрыто]@api.example.org/x' }
    )
end

-- ── Список атрибутов ─────────────────────────────────────────────────

g.test_attributes_are_listed_by_key_in_alphabetical_order = function()
    t.assert_equals(encode.attributes({ z = 1, a = 'x', m = true }), {
        { key = 'a', value = { stringValue = 'x' } },
        { key = 'm', value = { boolValue = true } },
        { key = 'z', value = { intValue = '1' } },
    })
end

g.test_no_attributes_means_no_list_rather_than_an_empty_array = function()
    -- Пустую таблицу JSON Tarantool пишет как `[]`; у protobuf отсутствие
    -- и пустота — одно, и поле честнее не писать.
    t.assert_equals(encode.attributes({}), nil)
end

-- ── Наносекунды ──────────────────────────────────────────────────────

g.test_nanos_are_a_decimal_string_without_the_ll_suffix = function()
    t.assert_equals(encode.nanos(helper.nanos(1700000000.25)), '1700000000250000000')
    t.assert_equals(encode.nanos(1700000000250000000LL), '1700000000250000000')
    t.assert_equals(encode.nanos(-5LL), '-5')
    t.assert_equals(encode.nanos(5ULL), '5')
end

g.test_a_plain_number_is_printed_as_an_integer_too = function()
    -- `tostring` дал бы экспоненту: 1.7e+18.
    t.assert_equals(encode.nanos(1700000000000000000), '1700000000000000000')
    t.assert_equals(encode.nanos(7), '7')
end

-- ── Отрезок ──────────────────────────────────────────────────────────

g.test_a_root_server_span_without_attributes_or_error_is_minimal = function()
    t.assert_equals(encode.span(helper.record()), {
        traceId = ('11'):rep(16),
        spanId = ('22'):rep(8),
        name = 'GET /customers/:id',
        kind = 2,
        startTimeUnixNano = '1700000000000000000',
        endTimeUnixNano = '1700000000250000000',
        flags = 3,
    })
end

g.test_a_child_span_carries_its_parent_state_attributes_and_error = function()
    local record = helper.record({
        span_id = ('33'):rep(8),
        parent_id = ('22'):rep(8),
        name = 'HTTP GET',
        kind = 'client',
        trace_flags = '01',
        tracestate = 'congo=t61rcWkgMzE',
        status = 'error',
        error = 'ответ 503',
        attributes = { ['http.response.status_code'] = 503, ['server.address'] = 'api.example.org' },
    })

    t.assert_equals(encode.span(record), {
        traceId = ('11'):rep(16),
        spanId = ('33'):rep(8),
        parentSpanId = ('22'):rep(8),
        traceState = 'congo=t61rcWkgMzE',
        name = 'HTTP GET',
        kind = 3,
        startTimeUnixNano = '1700000000000000000',
        endTimeUnixNano = '1700000000250000000',
        attributes = {
            { key = 'http.response.status_code', value = { intValue = '503' } },
            { key = 'server.address', value = { stringValue = 'api.example.org' } },
        },
        status = { code = 2, message = 'ответ 503' },
        flags = 1,
    })
end

g.test_each_kind_maps_to_its_number_and_an_unknown_one_to_zero = function()
    for name, number in pairs({ internal = 1, server = 2, client = 3, producer = 4, consumer = 5 }) do
        t.assert_equals(encode.span(helper.record({ kind = name })).kind, number, name)
    end

    t.assert_equals(encode.span(helper.record({ kind = 'weird' })).kind, 0)
end

g.test_flags_are_the_eight_bits_of_the_header = function()
    t.assert_equals(encode.span(helper.record({ trace_flags = '00' })).flags, 0)
    t.assert_equals(encode.span(helper.record({ trace_flags = 'ff' })).flags, 255)
end

g.test_secrets_in_the_name_and_the_error_are_hidden = function()
    local record = helper.record({
        name = 'POST https://ivan:hunter2@api.example.org/pay',
        status = 'error',
        error = 'password=hunter2 rejected',
    })
    local span = encode.span(record)

    t.assert_equals(span.name, 'POST https://ivan:[скрыто]@api.example.org/pay')
    t.assert_equals(span.status, { code = 2, message = 'password=[скрыто] rejected' })
end

-- ── Запрос целиком ───────────────────────────────────────────────────

g.test_the_request_holds_one_resource_one_scope_and_the_spans_in_order = function()
    local records = { helper.record({ name = 'first' }), helper.record({ name = 'second' }) }
    local resource = { ['service.name'] = 'panel', ['service.instance.id'] = 'panel-001-a' }

    local request = encode.request(records, resource)

    t.assert_equals(request, {
        resourceSpans = {
            {
                resource = {
                    attributes = {
                        { key = 'service.instance.id', value = { stringValue = 'panel-001-a' } },
                        { key = 'service.name', value = { stringValue = 'panel' } },
                    },
                },
                scopeSpans = {
                    {
                        scope = { name = 'tnt-trace' },
                        spans = { encode.span(records[1]), encode.span(records[2]) },
                    },
                },
            },
        },
    })
    t.assert_equals(request.resourceSpans[1].scopeSpans[1].spans[1].name, 'first')
    t.assert_equals(request.resourceSpans[1].scopeSpans[1].spans[2].name, 'second')
end

g.test_the_request_survives_json_with_ids_and_nanos_as_strings = function()
    -- Именно это коллектор проверяет строго: base64 в опознавателе — 400,
    -- а наносекунды числом теряли бы точность в double.
    local text =
        json.encode(encode.request({ helper.record({ attributes = { rows = 7 } }) }, { ['service.name'] = 'p' }))
    local decoded = json.decode(text)
    local span = decoded.resourceSpans[1].scopeSpans[1].spans[1]

    t.assert_equals(span.traceId, ('11'):rep(16))
    t.assert_equals(span.spanId, ('22'):rep(8))
    t.assert_equals(span.startTimeUnixNano, '1700000000000000000')
    t.assert_equals(span.endTimeUnixNano, '1700000000250000000')
    t.assert_equals(span.attributes[1].value.intValue, '7')
    t.assert_equals(text:find('"traceId":"' .. ('11'):rep(16) .. '"', 1, true) ~= nil, true)
    t.assert_equals(text:find('"startTimeUnixNano":"1700000000000000000"', 1, true) ~= nil, true)
end

g.test_an_empty_batch_is_a_resource_without_spans = function()
    local request = encode.request({}, { ['service.name'] = 'p' })

    t.assert_equals(request.resourceSpans[1].scopeSpans[1].spans, {})
end
