#!/usr/bin/env bash
# Поднимает коллектор OpenTelemetry для живых проверок выгрузки трасс.
#
# Образ grafana/otel-lgtm — коллектор, Tempo, Loki, Prometheus и Grafana
# в одном контейнере. Настоящий коллектор, а не двойник: двойник
# показывает, что мы правильно разговариваем сами с собой, а чужой
# коллектор — что нас понимает кто-то ещё. Разница вылезает на первом же
# опознавателе: base64 вместо шестнадцатеричной записи коллектор
# отвергает с 400, а трассу из одних нулей принимает с 200 и молча теряет
# соседний отрезок.
#
#   test/stand/otel.sh          # поднять
#   test/stand/otel.sh stop     # погасить
#
# Порты нестандартные, чтобы не спорить с коллектором, который уже
# работает на машине: OTLP/HTTP 14318, Tempo 13200, Grafana 13000.
# Посмотреть глазами: http://127.0.0.1:13000/ (admin / admin).
set -euo pipefail

cd "$(dirname "$0")"

OTEL_IMAGE='grafana/otel-lgtm:0.24.0'
OTEL_CONTAINER='tnt-stand-otel'
OTEL_OTLP_PORT='14318'
OTEL_TEMPO_PORT='13200'
OTEL_GRAFANA_PORT='13000'

if ! command -v docker > /dev/null 2>&1; then
    echo 'docker не найден: коллектор поднять нечем' >&2
    exit 1
fi

if [ "${1:-up}" = 'stop' ]; then
    docker rm -f "${OTEL_CONTAINER}" > /dev/null 2>&1 || true
    rm -f run/otel.container
    echo 'коллектор остановлен'
    exit 0
fi

mkdir -p run

# Повторный запуск безвреден: контейнер с тем же именем сносится
# и поднимается заново. Трассы стенда жалеть незачем.
docker rm -f "${OTEL_CONTAINER}" > /dev/null 2>&1 || true

docker run -d \
    --name "${OTEL_CONTAINER}" \
    -p "127.0.0.1:${OTEL_OTLP_PORT}:4318" \
    -p "127.0.0.1:${OTEL_TEMPO_PORT}:3200" \
    -p "127.0.0.1:${OTEL_GRAFANA_PORT}:3000" \
    "${OTEL_IMAGE}" > /dev/null

echo "${OTEL_CONTAINER}" > run/otel.container

# Готовности ждём: проверки, запущенные сразу после подъёма, иначе
# пропустятся — и это выглядит как «всё хорошо», хотя ничего
# не проверено. Готов и коллектор (принял пустую партию), и Tempo
# (в нём проверки ищут отправленный отрезок).
for _ in $(seq 1 240); do
    if curl -s -f -o /dev/null -X POST -H 'Content-Type: application/json' \
        -d '{"resourceSpans":[]}' "http://127.0.0.1:${OTEL_OTLP_PORT}/v1/traces" \
        && curl -s -f -o /dev/null "http://127.0.0.1:${OTEL_TEMPO_PORT}/ready"; then
        echo "коллектор поднят: OTLP http://127.0.0.1:${OTEL_OTLP_PORT}, Tempo http://127.0.0.1:${OTEL_TEMPO_PORT}"
        exit 0
    fi

    sleep 0.5
done

echo "коллектор не ответил за 120 секунд: смотрите docker logs ${OTEL_CONTAINER}" >&2
exit 1
