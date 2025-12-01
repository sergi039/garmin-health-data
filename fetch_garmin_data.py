#!/usr/bin/env python3
"""
Fetch health data from Garmin Connect and save as markdown for TypingMind Knowledge Base
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from garminconnect import Garmin

# Load credentials
load_dotenv()
EMAIL = os.getenv("GARMIN_EMAIL")
PASSWORD = os.getenv("GARMIN_PASSWORD")

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SESSION_FILE = Path(__file__).parent / ".garmin_session"


def get_client():
    """Initialize Garmin client with session caching"""
    client = Garmin(EMAIL, PASSWORD)

    # Try to load existing session
    if SESSION_FILE.exists():
        try:
            client.login(SESSION_FILE)
            return client
        except Exception:
            pass

    # Fresh login
    client.login()
    client.garth.dump(str(SESSION_FILE))
    return client


def fetch_and_save():
    """Fetch all health data and save to markdown"""
    client = get_client()
    today = datetime.now().date()

    # Collect data
    data = {
        "date": str(today),
        "updated_at": datetime.now().isoformat(),
    }

    # Daily stats
    try:
        stats = client.get_stats(today.isoformat())
        data["daily_stats"] = {
            "steps": stats.get("totalSteps"),
            "distance_km": round(stats.get("totalDistanceMeters", 0) / 1000, 2),
            "calories": stats.get("totalKilocalories"),
            "active_calories": stats.get("activeKilocalories"),
            "resting_heart_rate": stats.get("restingHeartRate"),
            "min_heart_rate": stats.get("minHeartRate"),
            "max_heart_rate": stats.get("maxHeartRate"),
            "stress_level": stats.get("averageStressLevel"),
            "body_battery_high": stats.get("bodyBatteryChargedValue"),
            "body_battery_low": stats.get("bodyBatteryDrainedValue"),
            "floors_climbed": stats.get("floorsAscended"),
            "intensity_minutes": stats.get("intensityMinutesGoal"),
        }
    except Exception as e:
        data["daily_stats_error"] = str(e)

    # Sleep data (last night)
    try:
        sleep = client.get_sleep_data(today.isoformat())
        if sleep and sleep.get("dailySleepDTO"):
            s = sleep["dailySleepDTO"]
            data["sleep"] = {
                "duration_hours": round(s.get("sleepTimeSeconds", 0) / 3600, 1),
                "deep_sleep_hours": round(s.get("deepSleepSeconds", 0) / 3600, 1),
                "light_sleep_hours": round(s.get("lightSleepSeconds", 0) / 3600, 1),
                "rem_sleep_hours": round(s.get("remSleepSeconds", 0) / 3600, 1),
                "awake_hours": round(s.get("awakeSleepSeconds", 0) / 3600, 1),
                "sleep_score": s.get("sleepScores", {}).get("overall", {}).get("value"),
            }
    except Exception as e:
        data["sleep_error"] = str(e)

    # Heart rate variability
    try:
        hrv = client.get_hrv_data(today.isoformat())
        if hrv and hrv.get("hrvSummary"):
            h = hrv["hrvSummary"]
            data["hrv"] = {
                "weekly_average": h.get("weeklyAvg"),
                "last_night": h.get("lastNight"),
                "status": h.get("status"),
            }
    except Exception as e:
        data["hrv_error"] = str(e)

    # Body composition (if available)
    try:
        body = client.get_body_composition(today.isoformat())
        if body and body.get("weight"):
            data["body"] = {
                "weight_kg": body.get("weight") / 1000 if body.get("weight") else None,
                "bmi": body.get("bmi"),
                "body_fat_percent": body.get("bodyFat"),
                "muscle_mass_kg": body.get("muscleMass") / 1000 if body.get("muscleMass") else None,
            }
    except Exception as e:
        data["body_error"] = str(e)

    # Recent activities (last 7 days)
    try:
        activities = client.get_activities(0, 10)
        data["recent_activities"] = []
        for act in activities[:5]:
            data["recent_activities"].append({
                "name": act.get("activityName"),
                "type": act.get("activityType", {}).get("typeKey"),
                "date": act.get("startTimeLocal", "")[:10],
                "duration_min": round(act.get("duration", 0) / 60, 1),
                "distance_km": round(act.get("distance", 0) / 1000, 2),
                "calories": act.get("calories"),
                "avg_hr": act.get("averageHR"),
                "max_hr": act.get("maxHR"),
            })
    except Exception as e:
        data["activities_error"] = str(e)

    # Save JSON
    json_file = DATA_DIR / "garmin_health.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Generate Markdown for Knowledge Base
    md = generate_markdown(data)
    md_file = DATA_DIR / "health_summary.md"
    with open(md_file, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Data saved to {json_file} and {md_file}")
    return data


def generate_markdown(data):
    """Generate readable markdown summary"""
    lines = [
        f"# Данные здоровья Garmin",
        f"",
        f"**Дата:** {data['date']}",
        f"**Обновлено:** {data['updated_at'][:16].replace('T', ' ')}",
        f"",
    ]

    # Daily stats
    if "daily_stats" in data:
        s = data["daily_stats"]
        lines.extend([
            "## Дневная активность",
            f"- **Шаги:** {s.get('steps', 'н/д')}",
            f"- **Дистанция:** {s.get('distance_km', 'н/д')} км",
            f"- **Калории:** {s.get('calories', 'н/д')} (активные: {s.get('active_calories', 'н/д')})",
            f"- **Пульс покоя:** {s.get('resting_heart_rate', 'н/д')} уд/мин",
            f"- **Пульс (мин/макс):** {s.get('min_heart_rate', 'н/д')} / {s.get('max_heart_rate', 'н/д')}",
            f"- **Уровень стресса:** {s.get('stress_level', 'н/д')}",
            f"- **Body Battery:** {s.get('body_battery_low', 'н/д')} - {s.get('body_battery_high', 'н/д')}",
            f"- **Этажи:** {s.get('floors_climbed', 'н/д')}",
            "",
        ])

    # Sleep
    if "sleep" in data:
        s = data["sleep"]
        lines.extend([
            "## Сон (прошлая ночь)",
            f"- **Общее время:** {s.get('duration_hours', 'н/д')} ч",
            f"- **Глубокий сон:** {s.get('deep_sleep_hours', 'н/д')} ч",
            f"- **Лёгкий сон:** {s.get('light_sleep_hours', 'н/д')} ч",
            f"- **REM:** {s.get('rem_sleep_hours', 'н/д')} ч",
            f"- **Бодрствование:** {s.get('awake_hours', 'н/д')} ч",
            f"- **Оценка сна:** {s.get('sleep_score', 'н/д')}",
            "",
        ])

    # HRV
    if "hrv" in data:
        h = data["hrv"]
        lines.extend([
            "## Вариабельность пульса (HRV)",
            f"- **Прошлая ночь:** {h.get('last_night', 'н/д')} мс",
            f"- **Недельный средний:** {h.get('weekly_average', 'н/д')} мс",
            f"- **Статус:** {h.get('status', 'н/д')}",
            "",
        ])

    # Body
    if "body" in data:
        b = data["body"]
        lines.extend([
            "## Состав тела",
            f"- **Вес:** {b.get('weight_kg', 'н/д')} кг",
            f"- **BMI:** {b.get('bmi', 'н/д')}",
            f"- **Жир:** {b.get('body_fat_percent', 'н/д')}%",
            f"- **Мышцы:** {b.get('muscle_mass_kg', 'н/д')} кг",
            "",
        ])

    # Activities
    if "recent_activities" in data and data["recent_activities"]:
        lines.extend([
            "## Последние тренировки",
            "",
        ])
        for act in data["recent_activities"]:
            lines.append(
                f"- **{act.get('date', '')} {act.get('name', '')}** ({act.get('type', '')}): "
                f"{act.get('duration_min', 0)} мин, {act.get('distance_km', 0)} км, "
                f"{act.get('calories', 0)} ккал, пульс {act.get('avg_hr', 'н/д')}/{act.get('max_hr', 'н/д')}"
            )
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    fetch_and_save()
