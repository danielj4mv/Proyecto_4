# Usar una imagen base de Python
FROM python:3.10-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copiar el código de la aplicación
COPY . .

# Exponer el puerto donde se ejecutará la aplicación
#EXPOSE 8087

# Comando para ejecutar la aplicación
CMD ["streamlit", "run", "interfaz.py", "--server.port=8087"]
