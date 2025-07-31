FROM public.ecr.aws/lambda/python:3.12

# Copier le fichier requirements.txt
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copier tout le code de ton projet
COPY . .

# Spécifier le point d'entrée : fichier lambda_function.py + nom de la fonction handler
CMD ["lambda_function.lambda_handler"]
