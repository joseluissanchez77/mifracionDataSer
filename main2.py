import psycopg2
from datetime import datetime
import json
import time

# Obtener el tiempo de inicio
start_time = time.time()

# Conexión a la base de datos PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="DB_JS_TIA",
    user="postgres",
    password="root"
)

# Crear un cursor
cursor = conn.cursor()

# Verificar si la tabla ya existe
table_exists_query = """
    SELECT EXISTS (
       SELECT 1
       FROM   information_schema.tables 
       WHERE  table_name = 'reviewsTWO'
    );
"""
cursor.execute(table_exists_query)
table_exists = cursor.fetchone()[0]

# Si la tabla no existe, créala
if not table_exists:
    # Definir la estructura de la tabla
    table_creation_query = """
        CREATE TABLE reviewsTWO (
            product_id VARCHAR(255),
            categories TEXT[],
            user_id VARCHAR(255),
            profile_name VARCHAR(255),
            helpfulness VARCHAR(10),
            score FLOAT,
            review_time TIMESTAMP,
            summary TEXT,
            review_text TEXT
        );
    """
    cursor.execute(table_creation_query)

# Inicializar una lista para almacenar los datos
batch_data = []

# Obtener el tiempo actual antes de comenzar el procesamiento
last_time = time.time()

# Abrir el archivo de texto output MuestraOutput
with open('C:/Users/Tierra77/Documents/PROYECTOS/PYTHON/MuestraOutput.txt', 'r', encoding='utf-8') as file:
    current_review = {}
    for line in file:
        line = line.strip()
        if line:
            key, value = map(str.strip, line.split(':', 1))
            current_review[key] = value

            if key == "review/time":
                current_review["review/time"] = datetime.utcfromtimestamp(int(current_review["review/time"]))

            if key == "review/text":

                # Asegurarse de que categories sea una lista, incluso si está vacía
                categories_str = current_review.get('product/categories', '[]')
                
                # Manejar el caso especial cuando categories_str es '[]'
                if categories_str == '[]':
                    categories = []
                else:
                    try:
                        categories = json.loads(categories_str)
                    except json.JSONDecodeError:
                        categories = []

    
               # Agregar datos al lote
                batch_data.append((
                    current_review.get('product/productId', ''),
                    categories,
                    current_review.get('review/userId', ''),
                    current_review.get('review/profileName', ''),
                    current_review.get('review/helpfulness', ''),
                    current_review.get('review/score', 0.0),
                    current_review.get('review/time', datetime.utcfromtimestamp(0)),
                    current_review.get('review/summary', ''),
                    current_review.get('review/text', '')
                ))

                # Si la lista del lote alcanza un tamaño específico (ajusta según sea necesario), realizar la inserción masiva
                if len(batch_data) >= 1000:
                    # Insertar datos en la tabla
                    cursor.executemany("""
                        INSERT INTO reviewsTWO 
                        (product_id, categories, user_id, profile_name, helpfulness, score, review_time, summary, review_text)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch_data)
                    # Limpiar la lista del lote para el próximo lote
                    batch_data = []

                # Reiniciar el diccionario para la próxima revisión
                #current_review = {}

                # Imprimir el tiempo transcurrido cada minuto
                current_time = time.time()
                elapsed_time = current_time - last_time
                if elapsed_time >= 60:  # Si ha pasado al menos 1 minuto
                    print(f"Registros procesados: {len(batch_data)}, Tiempo transcurrido: {elapsed_time:.2f} segundos")
                    last_time = current_time

# Insertar cualquier dato restante en el lote
if batch_data:
    cursor.executemany("""
        INSERT INTO reviewsTWO 
        (product_id, categories, user_id, profile_name, helpfulness, score, review_time, summary, review_text)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, batch_data)

# Confirmar los cambios y cerrar la conexión
conn.commit()
cursor.close()
conn.close()

# Obtener el tiempo de finalización
end_time = time.time()

# Calcular el tiempo total de ejecución
execution_time = end_time - start_time

# Calcular el tiempo total de ejecución
execution_time = (end_time - start_time) / 60  # Convertir a minutos


# Imprimir el tiempo total de ejecución
print(f"Tiempo total de ejecución: {execution_time} segundos")