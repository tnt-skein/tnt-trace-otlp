--- Настройки выгрузчика трасс: проверка, умолчания и клиент к коллектору.
---
--- Живут отдельно от выгрузки, потому что у них своя забота — отвергнуть
--- негодное на строке вызывающего и собрать клиент, которому нельзя
--- перетереть свои шесть настроек, — а у выгрузки своя: партии, повторы
--- и счётчики. Фасад `tnt.trace.otlp` отдаёт умолчания под прежними
--- именами, и вызывающий этого деления не видит.

local compress = require('tnt.compress')
local http = require('tnt.http')
local journal = require('tnt.log')
local must = require('tnt.must')

local Module = {}

--- Уровень вины: строка того, кто позвал функцию фасада.
---
--- Три, а не два: настройки проверяет помощник фасада, позванный
--- не хвостовым вызовом.
local CALLER_OF_HELPER = 3

--- Адрес коллектора, если не сказано иного: порт OTLP/HTTP.
Module.DEFAULT_ENDPOINT = 'http://127.0.0.1:4318'

--- Отрезков в партии, если не сказано иного.
Module.DEFAULT_BATCH = 512

--- Период такта в секундах, если не сказано иного.
Module.DEFAULT_INTERVAL = 1

--- Срок обращения к коллектору в секундах, если не сказано иного.
Module.DEFAULT_TIMEOUT = 5

--- Сколько раз повторять партию сверх первой попытки, если не сказано иного.
Module.DEFAULT_RETRIES = 5

--- Тело партии сжимается gzip и уходит с `Content-Encoding: gzip`.
Module.GZIP = 'gzip'

--- Тело партии уходит как есть.
Module.NONE = 'none'

--- Чем сжимать тело партии: имена — как у `compression` коллектора
--- и переменной `OTEL_EXPORTER_OTLP_COMPRESSION`.
Module.COMPRESSIONS = { Module.GZIP, Module.NONE }

--- Сжатие тела партии, если не сказано иного.
Module.DEFAULT_COMPRESSION = Module.GZIP

--- Атрибут ресурса с именем службы.
Module.SERVICE_NAME = 'service.name'

--- Описание настроек: незнакомая — отказ, а не молчаливое умолчание.
local SETTINGS_SPEC = {
    endpoint = '?not_empty',
    service_name = 'not_empty',
    resource = '?table',
    headers = '?table',
    client = '?table',
    batch = '?integer',
    interval = '?number',
    timeout = '?number',
    retries = '?integer',
    compression = { '?one_of', Module.COMPRESSIONS },
}

--- Настройки клиента, которые задаёт сам выгрузчик, — и чем их задают.
local OWN_CLIENT_SETTINGS = {
    base_url = 'адрес — настройкой endpoint',
    headers = 'заголовки — настройкой headers',
    timeout = 'срок — настройкой timeout',
    propagate = 'отправка партии не рождает отрезков и не несёт заголовков контекста',
    retry = 'повторы партии ведёт выгрузчик',
    max_redirects = 'коллектор не переезжает',
}

--- Атрибуты ресурса, которые задаются своей настройкой.
local OWN_RESOURCE = {
    [Module.SERVICE_NAME] = 'имя службы — настройкой service_name',
}

--- Заголовки, которые выгрузчик ставит каждой партии сам, — и почему.
---
--- Заголовок запроса главнее заголовка клиента, и такой заголовок
--- из `headers` пропал бы молча.
local OWN_HEADERS = {
    ['content-type'] = 'тело партии — JSON',
    ['content-encoding'] = 'сжатие — настройкой compression',
}

---@class TntTraceOtlpOptions
---@field endpoint string|nil Адрес коллектора, к нему дописывается `/v1/traces`; по умолчанию порт 4318 узла
---@field service_name string Имя службы в ресурсе (`service.name`)
---@field resource table<string, string|number|boolean>|nil Атрибуты ресурса сверх имени службы и инстанса
---@field headers table<string, string>|nil Заголовки на каждое обращение к коллектору, например `authorization`
---@field client table|nil Прочие настройки `tnt-http`: сертификаты, сокет, срок соединения
---@field batch integer|nil Отрезков в партии; по умолчанию 512
---@field interval number|nil Период такта, секунды; по умолчанию 1
---@field timeout number|nil Срок обращения к коллектору, секунды; по умолчанию 5
---@field retries integer|nil Повторов партии сверх первой попытки; по умолчанию 5
---@field compression string|nil Сжатие тела партии: `gzip` либо `none`; по умолчанию `gzip`

--- Бросает, если в таблице настроек задано то, что задаётся иначе.
---
--- Молча перетёртая настройка хуже отказа: вызывающий уверен, что его
--- `retry` действует, а выгрузчик его не видит.
---@param given table
---@param what string Чья это таблица — для сообщения
---@param own table<string, string> Имя → чем его задают
local function refuse_own(given, what, own)
    for name, hint in pairs(own) do
        if given[name] ~= nil then
            -- На единицу выше помощника настроек: этот зовётся из него.
            error(('%s.%s задаёт выгрузчик: %s'):format(what, name, hint), CALLER_OF_HELPER + 1)
        end
    end
end

--- Проверяет настройки старта с виной на строке вызывающего.
---@param opts any
---@return table
function Module.check(opts)
    local at = must.at(CALLER_OF_HELPER)
    local options = at.table(opts, 'настройки выгрузки трасс')

    at.options(options, 'настройки выгрузки трасс', SETTINGS_SPEC)
    at.optional.positive(options.batch, 'настройки выгрузки трасс.batch')
    at.optional.positive(options.interval, 'настройки выгрузки трасс.interval')
    at.optional.positive(options.timeout, 'настройки выгрузки трасс.timeout')
    at.optional.non_negative(options.retries, 'настройки выгрузки трасс.retries')

    local given_resource = options.resource or {}
    local given_client = options.client or {}

    for name, value in pairs(given_resource) do
        at.not_empty(name, 'настройки выгрузки трасс.resource: имя атрибута')
        at.kind(
            value,
            ('настройки выгрузки трасс.resource.%s'):format(name),
            'string|number|boolean'
        )
    end

    refuse_own(given_resource, 'настройки выгрузки трасс.resource', OWN_RESOURCE)
    refuse_own(given_client, 'настройки выгрузки трасс.client', OWN_CLIENT_SETTINGS)

    return {
        endpoint = options.endpoint or Module.DEFAULT_ENDPOINT,
        service_name = options.service_name,
        resource = given_resource,
        headers = options.headers,
        client = given_client,
        batch = options.batch or Module.DEFAULT_BATCH,
        interval = options.interval or Module.DEFAULT_INTERVAL,
        timeout = options.timeout or Module.DEFAULT_TIMEOUT,
        retries = options.retries or Module.DEFAULT_RETRIES,
        compression = options.compression or Module.DEFAULT_COMPRESSION,
    }
end

--- Отказ о заголовке вызывающего, который выгрузчик ставит сам, либо пусто.
---
--- Имена сверяются без регистра: клиент приводит их к нижнему. Имя
--- не строкой отвергнет сборка клиента — у неё свой отказ.
---@param headers table<any, any>|nil
---@return string|nil complaint
local function own_header_of(headers)
    for name in pairs(headers or {}) do
        local hint = type(name) == 'string' and OWN_HEADERS[name:lower()]

        if hint then
            return ('заголовок %s задаёт выгрузчик: %s'):format(name, hint)
        end
    end

    return nil
end

--- Отказ, если назначенного сжатия на узле нет, либо пусто.
---
--- Сверяется при старте: узел без zlib иначе бросал бы на каждом такте,
--- а партии пропадали бы без счёта.
---@param compression string
---@return string|nil complaint
local function compression_complaint(compression)
    if compression == Module.NONE then
        return nil
    end

    local present, why = compress.available()

    if present then
        return nil
    end

    return ('сжимать gzip нечем — %s; без сжатия — compression = «none»'):format(why)
end

--- Клиент к коллектору: настройки вызывающего и свои.
---
--- Схема адреса сверяется здесь, а не первой партией: клиент к `ftp://`
--- собирается и отказывает только на отправке, и узел молча повторял бы
--- партию до конца повторов, а потом выбрасывал бы каждую. Заголовки
--- и сжатие — тоже здесь и тоже отказом парой, а не броском, как отказ
--- самой сборки клиента: тот, кто сверил форму настроек заранее, такого
--- не видит и получает отказ значением, а не исключение посреди
--- применения.
---@param given table Проверенные настройки
---@return TntHttpClient|nil
---@return string|nil err
function Module.client(given)
    if not http.url.WEB[http.url.scheme(given.endpoint)] then
        return nil,
            ('адрес «%s» не годится: коллектор слушает http или https'):format(
                journal.scrub(given.endpoint)
            )
    end

    local complaint = own_header_of(given.headers) or compression_complaint(given.compression)

    if complaint ~= nil then
        return nil, complaint
    end

    local options = table.copy(given.client)

    options.base_url = given.endpoint
    options.headers = given.headers
    options.timeout = given.timeout
    options.propagate = false
    options.retry = { attempts = 1 }
    options.max_redirects = 0

    return http.new(options)
end

return Module
