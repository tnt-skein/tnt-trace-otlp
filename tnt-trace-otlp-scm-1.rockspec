rockspec_format = '3.0'

package = 'tnt-trace-otlp'
version = 'scm-1'

source = {
    url = 'git+https://github.com/tnt-skein/tnt-trace-otlp.git',
    branch = 'main',
}

description = {
    summary = 'Выгрузка трасс tnt-trace в коллектор OpenTelemetry по OTLP/HTTP JSON',
    detailed = [[
        Фоновый выгрузчик закрытых отрезков tnt-trace: забирает партии
        из очереди трассы и шлёт их коллектору OpenTelemetry по OTLP/HTTP
        в JSON — опознаватели шестнадцатеричной строкой, время десятичной
        строкой наносекунд, род и статус целыми. Ресурс — service.name
        из настройки и service.instance.id узла. Атрибуты и имена
        отрезков проходят правило тайн журнала: атрибут с именем тайны
        уходит скрытым.

        При старте собирает свой клиент tnt-http с propagate = false
        (отправка партии не рождает отрезка о самой себе), ставит крюки
        трассы trace.install — клиентских отрезков клиенту HTTP и отрезков
        producer и consumer загруженным очередям tnt-queue, tnt-event,
        tnt-amqp, tnt-kafka — и включает запись в tnt-trace. Цикл
        на tnt-loop: партия до 512 отрезков раз в секунду, а набравшаяся
        полная партия будит цикл, не дожидаясь такта. Узел, закрывающий
        отрезок, выгрузки не ждёт никогда.

        Ответы коллектора — по OTLP: 429, 502, 503, 504 и сеть — повтор
        той же партии с отступом по степени двойки и не раньше
        Retry-After, прочие отказы — партия выбрасывается и считается.
        Отправка одна за раз: такт и ручной flush ждут друг друга. Тело
        партии по умолчанию сжимается gzip (Content-Encoding: gzip)
        через tnt-compress; compression = 'none' шлёт как есть.

        Зависит от tnt-trace (очередь отрезков), tnt-http (клиент
        к коллектору), tnt-retry (правило Retry-After), tnt-loop (цикл
        выгрузки), tnt-compress (gzip), tnt-log (журнал и правило тайн),
        tnt-clock (монотонные часы повтора), tnt-must (проверки
        аргументов) и tnt-external (подмена часов и имени узла
        в проверках). Покрытие строк и убитых мутантов — 100 %.
    ]],
    homepage = 'https://github.com/tnt-skein/tnt-trace-otlp',
    issues_url = 'https://github.com/tnt-skein/tnt-trace-otlp/issues',
    maintainer = 'tnt-skein',
    license = 'MIT',
    labels = { 'tarantool', 'opentelemetry', 'otlp', 'tracing', 'observability' },
}

dependencies = {
    'lua >= 5.1',
    'tnt-must',
    'tnt-clock',
    'tnt-compress',
    'tnt-http',
    'tnt-log',
    'tnt-loop',
    'tnt-retry',
    'tnt-external',
    'tnt-trace',
}

build = {
    type = 'builtin',
    modules = {
        ['tnt.trace.otlp'] = 'tnt/trace/otlp.lua',
        ['tnt.trace.otlp.encode'] = 'tnt/trace/otlp/encode.lua',
        ['tnt.trace.otlp.reply'] = 'tnt/trace/otlp/reply.lua',
        ['tnt.trace.otlp.settings'] = 'tnt/trace/otlp/settings.lua',
    },
}
