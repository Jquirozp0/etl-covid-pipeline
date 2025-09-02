🔹 Guía de Instalación Local

---------------------------------

1️⃣ Clonar el proyecto

    git clone <URL_DEL_REPOSITORIO>
    cd <NOMBRE_DEL_PROYECTO>

2️⃣ Crear y activar el entorno virtual (.venv)

    En Windows (PowerShell):
        python -m venv .venv
        .\.venv\Scripts\Activate.ps1

    En Windows (CMD):
    python -m venv .venv
        .\.venv\Scripts\activate.bat
 
    En macOS/Linux:
        python3 -m venv .venv
        source .venv/bin/activate
    ⚠️ Cuando el entorno esté activo, tu terminal debería mostrar (.venv) al inicio.

3️⃣ Instalar dependencias

    pip install --upgrade pip
    pip install -r requirements.txt

4️⃣ Configurar variables de entorno (.env)

    BASE_API_URL=https://covid-api.com/api
    AWS_ACCESS_KEY_ID=tu_access_key
    AWS_SECRET_ACCESS_KEY=tu_secret_key
    AWS_DEFAULT_REGION=us-east-1
    S3_BUCKET_NAME=nombre_bucket
    COVID_DATE=2023-09-01
    COUNTRIES= MX,CO,PE en formato ISO-3166-1 alpha-2

5️⃣ Configurar logs

    mkdir -p logs   

6️⃣ Ejecutar el pipeline

    python src/run.py

8️⃣ Notas adicionales
    Para desactivar el entorno virtual:
    
        deactivate