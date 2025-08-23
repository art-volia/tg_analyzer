# Telegram Analyzer

## Быстрый старт (без Docker)
```bash
mkdir -p ~/tg-analyzer && cd ~/tg-analyzer
# загрузите архив и распакуйте сюда
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # впишите API_ID/API_HASH/SESSION_NAME
streamlit run dashboard_app.py --server.address 0.0.0.0 --server.port 8501
```
1) В панели (ENV) заполните ключи и сохраните.
2) Во вкладке «Настройки» добавьте чаты и лимиты → сохранить.
3) Во вкладке «Состояние» кнопками «Запустить»/«Остановить» управляйте воркером.

Панель работает независимо от воркера — интерфейс доступен даже при остановленном воркере.

## Проверка статуса воркера
- Цветовой индикатор в панели: зелёный — воркер активен, красный — остановлен.
- Файл `runtime/worker.pid`: если существует, воркер запущен.

## Обновление системы
```bash
git pull
pip install -r requirements.txt  # при необходимости обновить зависимости
systemctl restart tg-dashboard   # перезапустить панель
```

## Расширенная панель
```bash
streamlit run dashboard_plus.py --server.address 0.0.0.0 --server.port 8501
```

## Systemd (панель)
Скопируйте пример `SYSTEMD_DASHBOARD_EXAMPLE.txt` в `/etc/systemd/system/tg-dashboard.service`, поправьте пользователя/пути, затем:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tg-dashboard
```

## Docker
```bash
# отредактируйте .env (или заполните его через панель)
docker compose up -d --build
# панель будет на http://<IP>:8501
```

## Миграции
```bash
python migrate_v2.py data/db.sqlite  # обновление схемы SQLite до новой модели
# миграция SQLite -> Postgres (предварительно создайте БД и выполните postgres_schema.sql, если нужно)
python migrate.py data/db.sqlite "postgresql+psycopg2://user:pass@host:5432/tganalyzer"
```

## Примечания
- Соблюдайте ToS Telegram и местные законы о данных.
- Уважайте FLOOD_WAIT — проект настроен на «лайтовый» сбор.

