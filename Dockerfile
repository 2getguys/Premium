FROM python:3.10-slim

# Встановлюємо робочий каталог в контейнері
WORKDIR /app

# Копіюємо файл залежностей та встановлюємо їх
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо всі файли .py в робочий каталог /app
# Це включатиме main.py, auth.py, config.py, та інші ваші модулі
COPY *.py /app/

# Якщо у вас є підкаталоги з Python модулями, їх теж потрібно скопіювати, наприклад:
# COPY your_module_directory/ /app/your_module_directory/

# Встановлюємо змінну середовища для негайного виводу логів Python
ENV PYTHONUNBUFFERED=1

# Команда для запуску вашого додатка
CMD ["python", "main.py"] 