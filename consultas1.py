import happybase
import pandas as pd

try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")

    # 2. Crear la tabla con las familias de columnas
    table_name = 'netflix_data'
    families = {
        'basic': dict(),  # Información básica
        'performance': dict(),  # Popularidad, puntuación, etc.
        'release': dict()  # Fecha de lanzamiento
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)
    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print(f"Tabla '{table_name}' creada exitosamente")

    # 3. Cargar datos del CSV
    file_path = '/home/vboxuser/Netflix_movies_and_tv_shows_clustering.csv'  # >
    netflix_data = pd.read_csv(file_path)

    # Iterar sobre el DataFrame usando el índice
    for index, row in netflix_data.iterrows():
        # Generar row key basado en el índice
        row_key = f'netflix_{index}'.encode()
        # Organizar los datos en familias de columnas
        data = {
            b'basic:title': str(row['title']).encode(),
            b'basic:type': str(row['type']).encode(),  # Usamos 'type' en lugar>
            b'performance:rating': str(row['rating']).encode(),
            b'performance:duration': str(row['duration']).encode(),
            b'release:year': str(row['release_year']).encode(),
            b'release:country': str(row['country']).encode()
        }
        table.put(row_key, data)
    print("Datos cargados exitosamente")

    # 4. Consultas de ejemplo
    print("\n=== Algunos títulos en la base de datos ===")
    count = 0
    for key, data in table.scan():
        if count < 3:  # Mostrar los primeros 3
            print(f"\nID: {key.decode()}")
            print(f"Título: {data[b'basic:title'].decode()}")
            print(f"Tipo: {data[b'basic:type'].decode()}")
            print(f"Año: {data[b'release:year'].decode()}")
        count += 1

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Cerrar la conexión
    connection.close()
