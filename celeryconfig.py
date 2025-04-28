from celery.schedules import crontab

beat_schedule = {
    'recalculate-ratings-every-10-minutes': {
        'task': 'tasks.recalculate_ratings',
        'schedule': crontab(minute='*/1'),  # Каждую 1 минуту
    },
}