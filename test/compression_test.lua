--- Проверки сжатия тела партии: gzip по умолчанию, `none`, отказ старта
--- на узле без zlib и на заголовке, который выгрузчик ставит сам.
---
--- Сжатие — настоящее, `tnt-compress` из исходников над системной zlib:
--- тело сверяется байт в байт с тем, что даёт она же на третьем уровне.
--- Что коллектор такое тело читает, показывает `otlp_live_test.lua`.

local digest = require('digest')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local g, world = helper.group('tnt.trace.otlp.compression')

--- Сжатие из той же загрузки исходников, что и выгрузчик.
---@return any
local function compress()
    return helper.module('tnt.compress')
end

--- Отправляет столько отрезков одной партией и отдаёт, что ушло.
---@param count integer
---@param overrides table|nil Настройки старта
---@return table sent
local function shipped(count, overrides)
    local sent = helper.started(world.otlp, helper.accepting(1), overrides)

    helper.spans(world.trace, count)

    t.assert_equals(world.otlp.flush(), { { spans = count, ok = true, status = 200, rejected = 0 } })

    return sent
end

--- Имена отрезков отправленной партии по порядку.
---@param sent table[]
---@param index integer
---@return string[]
local function names_in(sent, index)
    local names = {}

    for _, span in ipairs(helper.body_of(sent, index).resourceSpans[1].scopeSpans[1].spans) do
        table.insert(names, span.name)
    end

    return names
end

--- Ставит сжатию загрузчик, который не находит ничего: узел без zlib.
local function without_zlib()
    compress()._set_source({
        load = function(name)
            error(('%s: cannot open shared object file'):format(name), 0)
        end,
    })
end

--- Чем отказывает старт на узле без zlib.
local NO_ZLIB = 'клиент к коллектору не собран: сжимать gzip нечем — нет системной библиотеки zlib: '
    .. 'z: cannot open shared object file; libz.so.1: cannot open shared object file; '
    .. 'без сжатия — compression = «none»'

-- ── Умолчание: gzip ──────────────────────────────────────────────────

g.test_the_compressions_are_those_of_the_collector = function()
    t.assert_equals(world.otlp.GZIP, 'gzip')
    t.assert_equals(world.otlp.NONE, 'none')
    t.assert_equals(world.otlp.COMPRESSIONS, { 'gzip', 'none' })
    t.assert_equals(world.otlp.DEFAULT_COMPRESSION, 'gzip')
    t.assert_equals(world.otlp.GZIP_LEVEL, 3)
end

g.test_the_batch_goes_gzipped_at_the_third_level_by_default = function()
    -- Партия в сотни отрезков со случайными опознавателями: на трёх
    -- отрезках с одними байтами соседние уровни zlib дают одно и то же,
    -- и уровень по телу было бы не отличить.
    world.trace.configure({ record = true, random = digest.urandom })

    local sent = shipped(300)
    local call = helper.at(sent, 1)
    local text = helper.text_of(sent, 1)
    local names = names_in(sent, 1)

    t.assert_equals(call.options.headers['content-encoding'], 'gzip')
    t.assert_equals(call.options.headers['content-type'], 'application/json')
    t.assert_equals({ #names, names[1], names[300] }, { 300, 'span-1', 'span-300' })

    -- Тело — JSON партии, сжатый третьим уровнем: байт в байт с тем, что
    -- даёт та же zlib, и не то, что дали бы соседние уровни и уровень
    -- по умолчанию.
    t.assert_equals(call.body, compress().deflate(text, { level = 3 }))

    for _, level in ipairs({ 2, 4, 6 }) do
        t.assert_not_equals(call.body, compress().deflate(text, { level = level }), level)
    end

    t.assert_lt(#call.body, #text)

    t.assert_equals(world.otlp.status().compression, 'gzip')
    t.assert_equals(
        world.journal.find('выгрузка трасс запущена').record.fields.compression,
        'gzip'
    )
end

g.test_a_retry_sends_the_same_compressed_bytes = function()
    local sent = helper.started(world.otlp, { helper.answer(503), helper.accepted() })

    helper.spans(world.trace, 2)
    world.otlp.flush()
    world.clock.advance(1)

    t.assert_equals(world.otlp.flush(), { { spans = 2, ok = true, status = 200, rejected = 0 } })
    t.assert_equals(helper.at(sent, 2).body, helper.at(sent, 1).body)
    t.assert_equals(helper.at(sent, 2).options.headers['content-encoding'], 'gzip')
end

-- ── Без сжатия ───────────────────────────────────────────────────────

g.test_none_sends_the_json_as_it_is = function()
    local sent = shipped(2, { compression = 'none' })
    local call = helper.at(sent, 1)

    t.assert_equals(call.options.headers['content-encoding'], nil)
    t.assert_equals(call.options.headers['content-type'], 'application/json')
    t.assert_equals(names_in(sent, 1), { 'span-1', 'span-2' })
    t.assert_equals(call.body:sub(1, 16), '{"resourceSpans"')

    t.assert_equals(world.otlp.status().compression, 'none')
    t.assert_equals(
        world.journal.find('выгрузка трасс запущена').record.fields.compression,
        'none'
    )
end

g.test_the_compression_of_the_moment_of_sending_is_used_for_a_pending_batch = function()
    -- Сжатие — настройка отправки, а не партии: ждущая партия после
    -- перенастройки уходит так, как велит новая.
    local sent = helper.started(world.otlp, { helper.answer(503), helper.accepted() })

    helper.spans(world.trace, 1)
    world.otlp.flush()

    t.assert_equals(helper.at(sent, 1).options.headers['content-encoding'], 'gzip')
    t.assert_equals(world.otlp.start(helper.settings({ compression = 'none' })), true)

    world.clock.advance(1)
    world.otlp.flush()

    t.assert_equals(helper.at(sent, 2).options.headers['content-encoding'], nil)
    t.assert_equals(helper.at(sent, 2).body, helper.text_of(sent, 1))
end

-- ── Отказы старта ────────────────────────────────────────────────────

g.test_an_unknown_compression_is_refused_on_the_caller_line = function()
    local err, place = helper.raised(world.otlp.start, { service_name = 'p', compression = 'zstd' })

    t.assert_equals(
        err,
        place
            .. 'настройки выгрузки трасс.compression — одно из «gzip», «none», а не «zstd»'
    )

    err, place = helper.raised(world.otlp.start, { service_name = 'p', compression = true })

    t.assert_equals(
        err,
        place
            .. 'настройки выгрузки трасс.compression — одно из «gzip», «none», а не true'
    )
    t.assert_equals(world.otlp.status().running, false)
end

g.test_a_node_without_zlib_refuses_gzip_and_ships_without_it = function()
    without_zlib()

    t.assert_equals({ world.otlp.start(helper.settings()) }, { nil, NO_ZLIB })
    t.assert_equals({ world.otlp.start(helper.settings({ compression = 'gzip' })) }, { nil, NO_ZLIB })
    t.assert_equals(world.otlp.status().running, false)
    t.assert_equals(world.trace.status().recording, false)

    -- Без сжатия zlib не нужна: выгрузка идёт, тело уходит как есть.
    local sent = shipped(1, { compression = 'none' })

    t.assert_equals(helper.at(sent, 1).options.headers['content-encoding'], nil)
    t.assert_equals(names_in(sent, 1), { 'span-1' })
end

g.test_a_refused_gzip_leaves_a_running_export_untouched = function()
    helper.started(world.otlp, helper.accepting(1), { compression = 'none', batch = 7 })
    without_zlib()

    t.assert_equals({ world.otlp.start(helper.settings()) }, { nil, NO_ZLIB })
    t.assert_equals(world.otlp.status().running, true)
    t.assert_equals(world.otlp.status().batch, 7)
    t.assert_equals(world.otlp.status().compression, 'none')
end

g.test_the_headers_the_exporter_sets_itself_are_refused = function()
    local cases = {
        { name = 'content-encoding', hint = 'сжатие — настройкой compression' },
        { name = 'Content-Encoding', hint = 'сжатие — настройкой compression' },
        { name = 'content-type', hint = 'тело партии — JSON' },
        { name = 'CONTENT-TYPE', hint = 'тело партии — JSON' },
    }

    helper.started(world.otlp, helper.accepting(1), { batch = 7 })

    for _, case in ipairs(cases) do
        t.assert_equals({ world.otlp.start(helper.settings({ headers = { [case.name] = 'identity' } })) }, {
            nil,
            ('клиент к коллектору не собран: заголовок %s задаёт выгрузчик: %s'):format(
                case.name,
                case.hint
            ),
        })
    end

    -- Идущая выгрузка осталась как была.
    t.assert_equals(world.otlp.status().running, true)
    t.assert_equals(world.otlp.status().batch, 7)
end

g.test_a_header_named_not_by_a_string_is_left_to_the_client = function()
    -- Сверка своих заголовков не бросает на имени не строкой: такое
    -- имя отвергает сборка клиента, и её отказ доходит как есть.
    t.assert_equals({ world.otlp.start(helper.settings({ headers = { [1] = 'x' } })) }, {
        nil,
        'клиент к коллектору не собран: настройка headers[1]: имя ключа должно быть строкой, а не 1',
    })
end

g.test_other_headers_go_along_with_the_own_ones = function()
    local sent = shipped(1, { headers = { authorization = 'Bearer abc', ['x-scope-orgid'] = 'tenant' } })
    local headers = helper.at(sent, 1).options.headers

    t.assert_equals(headers['authorization'], 'Bearer abc')
    t.assert_equals(headers['x-scope-orgid'], 'tenant')
    t.assert_equals(headers['content-encoding'], 'gzip')
    t.assert_equals(headers['content-type'], 'application/json')
end
