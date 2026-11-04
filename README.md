# «Покупай выгодно» — аналитическая платформа

Учебный стенд маркетплейса. Магазины присылают товары, поток отбрасывает запрещённые, очищенные данные зеркалируются во второй кластер Kafka, складываются в HDFS и считаются в Spark. Клиент ищет товар и читает рекомендации. Канал до брокеров закрыт TLS, топики закрыты ACL.

## Как устроен поток

```text
data/products.json
        │  SHOP API, сертификат shop
        ▼
shop.products          кластер A, 3 брокера, RF=3, min.insync.replicas=2
        │  Kafka Streams, сертификат streams
        │  левое соединение с KTable shop.banned
        ▼
shop.products.clean
        ├─ Kafka Connect FileStreamSink → data/runtime/products-clean.jsonl
        └─ MirrorSourceConnector (MM2) → кластер B
                                              │
                                              ▼
                                   primary.shop.products.clean
                                              │
                                              ▼
                                         HDFS /data/products
                                              │
                                              ▼
                                         Spark: топ-3 по остатку
                                              │
                                              ▼
                                         топик recommendations
```

Клиентские команды пишут события в `client.searches` и `client.recommendation-requests`. Поиск читает файл, который собрал Connect. Рекомендации читаются из топика `recommendations`.

Запрещённый товар в учебном наборе — `99999`, «Охотничий нож». У него самый большой остаток, поэтому если фильтр не сработает, Spark поставит его первым. В рекомендациях его быть не должно.

## Что стоит на стенде

| Часть | Чем сделано |
|---|---|
| Кластер A | Apache Kafka 3.7.1, KRaft, брокеры `kafka-a1` `kafka-a2` `kafka-a3` |
| Кластер B | один брокер `kafka-b1`, сюда MM2 копирует уже отфильтрованные топики |
| TLS | свой CA, клиентский сертификат обязателен, личность берётся из CN |
| ACL | `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false` |
| Фильтр | Kafka Streams |
| Хранилище для отладки | Kafka Connect `FileStreamSink` |
| Копия во второй кластер | отдельный процесс MirrorMaker 2, приёмник `localhost:19192` |
| Data Lake | HDFS, один namenode и один datanode, фактор репликации 1 |
| Аналитика | Spark 3.5, локальный режим, три товара с наибольшим остатком |
| Метрики | JMX Exporter на каждом брокере, Prometheus, Grafana, Alertmanager |

Серые пункты задания (ksqlDB, Elasticsearch, Telegram) не входят в этот стенд: для сдачи хватает базового пути.

Пароль учебных ключей — `your-password`. Это стендовый пароль, не секрет продакшена. Каталог `secrets/` генерируется скриптом и в git не коммитится.

## Порты

| Сервис | Адрес |
|---|---|
| Брокеры A | `localhost:19092`, `localhost:19094`, `localhost:19096` |
| Брокер B | `localhost:19192` |
| JMX Exporter | `localhost:17071` … `17074` |
| Kafka Connect | `http://localhost:8083` |
| HDFS Web UI | `http://localhost:9870` |
| Prometheus | `http://localhost:9090` |
| Alertmanager | `http://localhost:9093` |
| Grafana | `http://localhost:3000` (анонимный просмотр, админ `admin` / `admin`) |
| Приёмник алертов | `http://localhost:9098/alerts`, пишет `evidence/alerts.log` |

## Топики кластера A

Все с `replication.factor=3` и `min.insync.replicas=2`.

| Топик | Кто пишет | Зачем |
|---|---|---|
| `shop.products` | `User:shop` | сырые товары магазинов |
| `shop.banned` | оператор, команда `ban` | компактный список запретов, ключ = `product_id` |
| `shop.products.clean` | Kafka Streams | товары после фильтра |
| `client.searches` | `User:client` | поисковые запросы |
| `client.recommendation-requests` | `User:client` | просьба выдать рекомендации |
| `recommendations` | задание Spark | три рекомендации |
| `connect-configs`, `connect-offsets`, `connect-status` | Connect | служебные, cleanup compact |

Магазину явно запрещена запись в `shop.products.clean` и `recommendations`. Клиенту запрещена запись в `shop.products`. Проверка: `java -jar app\build\libs\marketplace.jar prove-acl`.

`User:connect` и `User:streams` стоят в `super.users`: Connect создаёт служебные топики MM2, Streams — changelog. Ограничение на магазины и клиентов от этого не снимается.

На кластере B фактор репликации 1 и `min.insync.replicas=1`: брокер один, это приёмник копии, а не второй кворум.

## Команды приложений

Сборка: `app\build\libs\marketplace.jar`. Рабочий каталог — корень проекта.

```text
java -jar app\build\libs\marketplace.jar shop
java -jar app\build\libs\marketplace.jar ban add 99999
java -jar app\build\libs\marketplace.jar ban remove 99999
java -jar app\build\libs\marketplace.jar ban list
java -jar app\build\libs\marketplace.jar filter
java -jar app\build\libs\marketplace.jar client search часы
java -jar app\build\libs\marketplace.jar client recommend
java -jar app\build\libs\marketplace.jar prove-acl
```

`lake` читает зеркало на `localhost:19192` и кладёт JSON в HDFS через WebHDFS. `publish` забирает результат Spark и пишет топик `recommendations`.

## Запуск

Скрипты сами находят корень репозитория. Каталог с Kafka, Hadoop, Spark, Prometheus, Grafana, Alertmanager и Gradle задаётся переменной `MARKETPLACE_TOOLS`, JDK — переменной `JAVA_HOME`. Свои пути удобно записать в `scripts/local.ps1` (образец — `scripts/local.ps1.example`, сам файл в git не входит). Если `JAVA_HOME` уже стоит в системе, а `gradle` есть в `PATH`, файл нужен только ради `MARKETPLACE_TOOLS`.

В `MARKETPLACE_TOOLS` ожидаются каталоги `kafka_2.13-3.7.1`, `hadoop-3.4.1`, `spark-3.5.3-bin-hadoop3`, `prometheus-2.54.1.windows-amd64`, `alertmanager-0.27.0.windows-amd64` и `grafana*`. Если их нет, `demo.ps1` распакует архивы из `downloads` внутри того же каталога. Агент JMX Exporter скачивается в `monitoring`, если его ещё нет.

Конфиги Connect, MirrorMaker, файловых sink, Grafana и HDFS скрипт собирает из `config/templates` в `data/runtime/generated`. В шаблонах путь проекта — плейсхолдер `{{ROOT}}`, в git нет абсолютных путей машины.

Продюсеры магазина, клиента, Spark и фильтра оставляют идемпотентность Kafka по умолчанию и просят `acks=all`. Явно выключать её не нужно: при повторе отправки ключ товара не должен плодить дубли.

`docker-compose.yml` для этого запуска не требуется. Если поднимать профили `lake` и `app`, в `.env` нужно задать `MARKETPLACE_TOOLS` (образец — `.env.example`). Каталог проекта монтируется как `./`.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\demo.ps1
```

Скрипт выпускает сертификаты, собирает jar, поднимает четыре брокера KRaft и HDFS, включает Connect, прогоняет товарный файл, Spark и клиентские команды, останавливает процесс `kafka-a3` и ждёт алерт `KafkaBrokerDown`, затем возвращает брокер.

Остановка:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\down.ps1
```

`down.ps1` ищет `docker` в `PATH` и в стандартных каталогах Docker Desktop. Если Docker не установлен, скрипт просто останавливает процессы Windows.

Дашборд Grafana называется `Kafka marketplace`.

## Что видно по прогону

В `evidence/` лежат результаты уже выполненного прогона, остальное (журналы процессов, сырой runtime) в git не входит.

| Файл | Что подтверждает |
|---|---|
| `evidence/products-clean.jsonl` | в файл попали разрешённые товары, `99999` среди них нет |
| `evidence/prove-acl.txt` | магазину отказано в записи в `shop.products.clean` |
| `evidence/alerts.log` | Alertmanager прислал `KafkaBrokerDown` для `kafka-a3` и затем `resolved` |
| `evidence/alertmanager-alerts.json` | тот же алерт в API Alertmanager |
| `evidence/grafana-dashboard.png` | дашборд Grafana с метриками брокеров |

## Как устроена фильтрация

`shop.banned` — компактный топик. Команда `ban add` пишет значение `banned`, `ban remove` пишет tombstone. Streams делает `leftJoin` потока `shop.products` с этой таблицей по ключу `product_id`. Если запись запрета есть, товар в `shop.products.clean` не попадает. Connect и MM2 читают уже очищенный топик, поэтому запрещённый товар не попадает ни в файл, ни в HDFS.

Список меняется той же командой `ban` без перезапуска фильтра.

## Как считается рекомендация

Spark читает JSON из `hdfs://localhost:9000/data/products`, сортирует по `stock.available` и оставляет три строки. Это не модель рекомендаций: в задании достаточно любого вычисления. Результат пишется в HDFS `/data/recommendations`, отдельная программа перекладывает его в топик `recommendations`.
