import concurrent.futures
import json


def load_race_data():
    with open('race_data.json', 'r', encoding='UTF-8') as file:
        return json.load(file)


prize_cache = {}


def load_prize_data(category):
    if category not in prize_cache:
        prize_file = f'prizes_list_{category}.txt'
        prizes = {}
        with open(prize_file, 'r', encoding='UTF-8') as file:
            for line in file:
                place, prize = line.strip().split(maxsplit=1)
                prizes[int(place)] = prize.replace("место ", "")
        prize_cache[category] = prizes
    return prize_cache[category]


def calculate_time(start_time, finish_time):
    start_h, start_m, start_s = map(int, start_time.split(':'))
    finish_h, finish_m, finish_s = map(int, finish_time.split(':'))

    start_seconds = start_h * 3600 + start_m * 60 + start_s
    finish_seconds = finish_h * 3600 + finish_m * 60 + finish_s

    if finish_seconds < start_seconds:
        finish_seconds += 86400

    total_seconds = finish_seconds - start_seconds
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def process_category(category, athletes):
    prizes = load_prize_data(category)
    athletes.sort(key=lambda x: (x['Время'], x['Нагрудный номер']))

    for i, athlete in enumerate(athletes, start=1):
        athlete['Место'] = i
        if i <= 49 and i in prizes:
            athlete['Приз'] = prizes[i]
        elif 'Приз' in athlete:
            del athlete['Приз']

    with open(f'{category}.json', 'w', encoding='UTF-8') as file:
        json.dump(athletes, file, ensure_ascii=False, indent=4)


def main():
    race_data = load_race_data()
    categories = ['M15', 'M16', 'M18', 'W15', 'W16', 'W18']
    category_athletes = {category: [] for category in categories}

    for athlete in race_data:
        category = athlete['Категория']
        if category in category_athletes:
            time = calculate_time(athlete['Время старта'], athlete['Время финиша'])
            category_athletes[category].append({
                'Нагрудный номер': athlete['Нагрудный номер'],
                'Имя и Фамилия': f"{athlete['Имя']} {athlete['Фамилия']}",
                'Время': time
            })

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_category, category, category_athletes[category])
            for category in categories
        ]
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()