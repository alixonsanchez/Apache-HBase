import happybase

# 1. Función para consultar una fila específica por su row_key
def query_data_from_hbase(connection, row_key):
    try:
        table = connection.table('netflix_data')
        row = table.row(row_key.encode('utf-8'))  # Codificar el row_key a bytes
        if row:
            print(f"\nDatos de la película con show_id {row_key}:")
            for column, value in row.items():
                print(f"{column.decode('utf-8')}: {value.decode('utf-8')}")
        else:
            print(f"No se encontraron datos para show_id {row_key}")
    except Exception as e:
        print(f"Error al consultar los datos: {e}")

# 2. Función para filtrar datos, por ejemplo, películas con rating mayor a 8
def filter_data_from_hbase(connection):
    try:
        table = connection.table('netflix_data')
filter_condition = "SingleColumnValueFilter('cf', 'rating', >, 'binary:>
        rows = table.scan(filter=filter_condition)

        print("\nPelículas con rating mayor a 8:")
        for row_key, row_data in rows:
            print(f"Row Key: {row_key.decode('utf-8')}")
            for column, value in row_data.items():
                print(f"{column.decode('utf-8')}: {value.decode('utf-8')}")
            print("-----")
    except Exception as e:
        print(f"Error al filtrar los datos: {e}")

# 3. Función para recorrer todas las filas de la tabla
def scan_all_data_from_hbase(connection):
    try:
        table = connection.table('netflix_data')
        rows = table.scan()

        print("\nRecorrido de todas las películas:")
        for row_key, row_data in rows:
            print(f"\nRow Key: {row_key.decode('utf-8')}")
            for column, value in row_data.items():
                print(f"{column.decode('utf-8')}: {value.decode('utf-8')}")
            print("-----")
    except Exception as e:
        print(f"Error al recorrer los datos: {e}")
