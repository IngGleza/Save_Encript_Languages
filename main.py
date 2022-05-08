# Prueba Técnica Tangelo Fintech - Cristian Gonzalez

from functions.func import getLanguagesCountries, createObjToTable, createDataFrame, createDatabase, insertResults
from functions import database
import os


# Global Variables
path = os.path.dirname(os.path.abspath(__file__))
gestorDB = None
data = None
objToInsertIntoDB = None

# Principal Function


def main():
    print("Comenzando. . . . \n")
    # First create path to save the documents
    try:
        os.mkdir(path + '\DATA')
    except:
        # if exist, pass
        pass
    print("Directorio para guardar los archivos creado \n")

    # Create the Database
    try:
        gestorDB = createDatabase(database)
    except Exception as e:
        print(e)
        raise e
    print("Base de datos creada exitosamente \n")

    # Get data from request
    try:
        data = getLanguagesCountries()
    except Exception as e:
        print(e)
        raise e
    print("Consulta de informacion realizada \n")
    # Create the DataFrame
    try:
        objToInsertIntoDB = createDataFrame(createObjToTable(data), path)
    except Exception as e:
        print(e)
        raise e
    print("Informacion procesada correctamente \n")

    # Insert objet into Database
    try:
        insertResults(objToInsertIntoDB, gestorDB)
    except Exception as e:
        print(e)
        raise e
    print("Informacion almacenada en la base de datos exitosamente \n")

    print("Termino exitosamente \n")

if __name__ == "__main__":
    main()
