# Основание для demo taxonomy

Этот документ фиксирует, откуда взяты demo-типы узлов/связей в seed-данных проекта. Они нужны для воспроизводимого smoke/demo сценария и не являются заявлением, что у заказчика production schema должна называться точно так же.

## Важное правило

- Runtime dictionary не хардкодит demo-типы. Endpoint `GET /api/v1/graph/dictionary` читает фактические distinct values из `g_nodes` и `g_edges`.
- Demo seed использует обобщенную финансовую graph vocabulary, близкую к LDBC FinBench.
- При подключении реальной или около-реальной БД источником истины должны быть данные/справочники заказчика: `node_type`, `edge_type`, `relation_family`, `g_identifiers`.

## Внешние источники

Основная публичная опора - LDBC Financial Benchmark (FinBench):

- Graph Data Council / LDBC описывает FinBench как benchmark для financial scenarios such as anti-fraud and risk control: <https://ldbcouncil.org/benchmarks/finbench/>.
- Официальная спецификация LDBC FinBench говорит, что частые финансовые сущности включают accounts, medium, persons, companies, loans, а связи отражают финансовую активность, например transfer средств между account-ами: <https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf>.
- Спецификация описывает сущности `Person`, `Company`, `Account`, `Loan`, `Medium` и связи `own`, `transfer`, `signIn`, `invest`, `apply`, `deposit`, `repay`, `withdraw`, `guarantee`.
- VLDB paper по LDBC FinBench объясняет, что финансовый граф обычно моделируется как directed graph: вершины - persons, companies, accounts; ребра - owning, transferring, guaranteeing. Paper также говорит, что schema получена через абстракцию production financial graphs: <https://www.vldb.org/pvldb/vol18/p3007-qi.pdf>.

## Mapping в проекте

| Demo value в проекте | Основание в FinBench / financial graph vocabulary | Комментарий |
| --- | --- | --- |
| `PERSON` | `Person` entity | Клиент/физлицо, seed по `party_rk`, `person_id`, `phone_no`. |
| `COMPANY` | `Company` entity | Юрлицо/организация. |
| `ACCOUNT` | `Account` entity | Банковский/платежный счет. |
| `DEVICE` | Близко к FinBench `Medium` | В проекте названо `DEVICE`, потому что demo хранит device/IP evidence; по смыслу это login/payment medium. |
| `CUSTOMER_OWNERSHIP` / `OWNS` | FinBench `own` relation: person/company owns account | Связь клиент/юрлицо -> account. |
| `ACCOUNT_FLOW` / `TRANSFERS_TO` | FinBench `transfer` relation: account transfers funds to account | Денежный поток между account-ами. |
| `CORPORATE_CONTROL` / `BENEFICIAL_OWNS` | Близко к FinBench `invest` и ownership/control patterns | Упрощенная demo-связь бенефициарного владения. Для production лучше заменить на справочник заказчика. |
| `SHARED_INFRASTRUCTURE` / `USES_DEVICE` | Близко к FinBench `signIn` через `Medium` | Общая инфраструктура входа/устройства/IP. |
| `PERSON_KNOWS_PERSON`, `PERSON_RELATIVE_PERSON`, `PERSON_SAME_CITY_PERSON` | Не core FinBench financial flow; demo investigation/social evidence layer | Оставлено для 1-hop/shortest-path demo. Для production включать только если такие evidence relation реально есть. |

## Вывод

Типы не должны протекать в реальную интеграцию как обязательная онтология. Они защищены как demo vocabulary на основе публичного FinBench/financial-graph подхода. Backend остается schema-light: он работает с generic `node_type`, `edge_type`, `relation_family` и возвращает словарь из текущих данных.
