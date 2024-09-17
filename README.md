## Описание
Коллеги начали вычислять координаты событий (сообщений, подписок, реакций, регистраций), которые совершили пользователи соцсети. Значения координат будут появляться в таблице событий. Пока определяется геопозиция только исходящих сообщений, но уже сейчас можно начать разрабатывать новый функционал.

В продукт планируют внедрить систему рекомендации друзей. Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:
состоят в одном канале,
раньше никогда не переписывались,
находятся не дальше 1 км друг от друга.
Нужно лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:
- Выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки.
- Посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей.
- Определить, как часто пользователи путешествуют и какие города выбирают.
Благодаря такой аналитике в соцсеть можно будет вставить рекламу: приложение сможет учитывать местонахождение пользователя и предлагать тому подходящие услуги компаний-партнёров. 

В таблицу событий добавились два поля — широта и долгота исходящих сообщений. Обновлённая таблица уже находится в HDFS по этому пути: /user/master/data/geo/events.
Есть координаты городов Австралии, которые аналитики собрали в одну таблицу — geo.csv.
