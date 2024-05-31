# Entrega Proyecto 3
Video explicativo en youtube de funcionamiento del proyecto: [https://youtu.be/yds9bxsD4N0](https://youtu.be/yds9bxsD4N0)
## Guía para desplegar 
1. **Clonar el repositorio en la carpeta desde la que se quiere ejecutar el notebook**
   ```console
   git clone https://github.com/danielj4mv/proyecto_4.git
   ```
2. **Ingresar desde la terminal a la carpeta en que se encuentra el archivo `docker-compose.yml`**
   ```docker
   cd Proyecto_4
   ```
3. **Crear carpetas necesarias para los volúmenes**
   ```console
   mkdir ./airflow/logs
   mkdir ./airflow/plugins
   mkdir ./data/output_data
   ```
4. **Crear la siguiente variable de entorno para poder modificar volúmenes**
   ```console
   echo -e "AIRFLOW_UID=$(id -u)" >> .env
   ```
5. **Crear y ejecutar los servicios establecidos en el `docker-compose.yml`**

   ```docker
   docker compose up airflow-init -d
   docker compose up -d
   ```
   Este proceso puede tomar varios minutos, espere a que termine de ejecutar para pasar al siguiente paso

6. **Una vez se ha terminado de ejecutar el comando anterior, puede proceder a interactuar con los servicios del docker-compose a través de sus apis:**

   - **Airflow:** puerto 8080, las credenciales de acceso están definidas en el `.env`
   - **MLflow:** puerto 8081
   - **Minio:** puerto 8089, las credenciales de acceso están definidas en el `.env`
   - **Fastapi:** puerto 8085
   - **Streamlit:** puerto 8085
     
   Recuerde que si el ingreso es dessde la máquina virtual debe ir a `IP_de_la_MV:Puerto`, desde el pc local sería `Localhost:Puerto`

## Github Action
   Recuerde que si el ingreso es dessde la máquina virtual debe ir a `IP_de_la_MV:Puerto`, desde el pc local sería `Localhost:Puerto`
Adicionalmente, este repositorio cuenta con un github action que se ejecuta cada que se realiza un push en el que se realizan cambios en alguna de las subacrpetas de la carpeta `Dockerfiles`, Este github action se encarga de construir las imágenes a partir de los Dockerfiles de cada una de las carpetas y publicarlos en Dockerhub automáticamente. Puede visualizar el código de este github action en `.github/workflows/main.yml`.
