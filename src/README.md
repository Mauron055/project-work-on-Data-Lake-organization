### Структура хранилища

/user/master/data/
├── geo/
│   └── geo.csv
├── raw/
│   ├── events/
│   │   └── events.parquet
│   └── geo/
│       └── geo.parquet
├── processed/
│   ├── user_profile/
│   │   └── user_profile.parquet
│   └── zone/
│       └── zone_events.parquet
└── analysis/
    └── friend_recommendation/
        └── friend_recommendation.parquet


Описание:
/user/master/data/raw/ - директория для хранения необработанных данных.
  /user/master/data/raw/events/ - хранилище для таблицы событий (events.parquet).
  /user/master/data/raw/geo/ - хранилище для таблицы геоданных (geo.parquet).
/user/master/data/processed/ - директория для хранения обработанных данных.
  /user/master/data/processed/user_profile/ - хранилище для витрины user_profile.parquet.
  /user/master/data/processed/zone/ - хранилище для витрины zone_events.parquet.
/user/master/data/analysis/ - директория для хранения данных для анализа.
  /user/master/data/analysis/friend_recommendation/ - хранилище для витрины friend_recommendation.parquet.

Частота обновления данных:
events.parquet: Ежедневно.
geo.parquet: По мере обновления геоданных.
user_profile.parquet: Ежедневно.
zone_events.parquet: Ежедневно.
friend_recommendation.parquet: Ежедневно.

Формат данных:
events.parquet: Parquet.
geo.parquet: Parquet.
user_profile.parquet: Parquet.
zone_events.parquet: Parquet.
friend_recommendation.parquet: Parquet.
