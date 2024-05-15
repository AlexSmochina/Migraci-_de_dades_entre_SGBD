import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

//Base de datos
interface School
// Alumnos
data class Alumno(val dni: String, val apenom: String, val direc: String, val pobla: String, val telef: String): School

// Notas
data class Nota(val dni: String, val cod: Int, val nota: Int): School

// Asignaturas
data class Asignatura(val cod: Int, val nombre: String): School

// COMPONENTES

class Potgres {
    private var connection: Connection? = null
    private val resultSet = mutableMapOf<String, ResultSet>()

    fun connexioBD(host: String, usuari: String? = null, password: String? = null, bd: String) {

        val url = "jdbc:postgresql://${host}/${bd}"
        if (usuari == null) {
            this.connection = DriverManager.getConnection(url)
        } else {
            this.connection = DriverManager.getConnection(url, usuari, password)
        }

    }

    fun llegeix(taula: String) {
        val query = connection?.createStatement() ?: throw Exception("La conneccion no existe")
        val result = query.executeQuery("SELECT * FROM $taula")

        resultSet[taula] = result
    }

    fun hiha(taula: String): Boolean {
        val boolean = resultSet[taula]?.next() ?: false

        return boolean
    }

    fun recuperar(taula: String): School? {
        val rs: ResultSet = resultSet[taula]!!
        var respuesta :School? = null

        when(taula){
            "asignaturas" -> {
                respuesta = recuperarAsignatura(rs)
            }
            "alumnos" -> {
                respuesta = recuperarAlumno(rs)
            }
            "notas" -> {
                respuesta = recuperarNotas(rs)
            }
        }
        return respuesta
    }

    //metodos para pedir los datos de cada tabla
    private fun recuperarAlumno(rs: ResultSet): Alumno {
        val alumno: Alumno = Alumno(
            dni = rs.getString("dni"),
            apenom = rs.getString("apenom"),
            direc = rs.getString("direc"),
            pobla = rs.getString("pobla"),
            telef = rs.getString("telef")

        )

        return alumno
    }

    private fun recuperarAsignatura(rs: ResultSet): Asignatura {
        val asignatura: Asignatura = Asignatura(
            cod = rs.getInt("cod"),
            nombre = rs.getString("nombre")
        )

        return asignatura
    }

    private fun recuperarNotas(rs: ResultSet): Nota {
        val nota: Nota = Nota(
            dni = rs.getString("dni"),
            cod = rs.getInt("cod"),
            nota = rs.getInt("nota")
        )

        return nota
    }

    fun desconnexio() {
        connection?.close() ?: throw Exception("La conneccion no existe")
    }
}

class Mongo {
    private  var mongoClient: MongoClient? = null
    private var database:  MongoDatabase? = null

    fun connexioMongoDb(host: String, usuari: String, password: String, bd: String) {
        val url = "mongodb+srv://${usuari}:${password}@${host}/?retryWrites=true&w=majority&appName=${bd}"
        mongoClient = MongoClients.create(url)
        database = mongoClient!!.getDatabase(bd)
    }


    fun insereix(colleccio: String, objecte: School) {
        val document = Document()
        val collection:  MongoCollection<Document>

        if (database == null) {
            throw Exception("No se ha creado la conneccion")
        } else {
            collection = database!!.getCollection(colleccio)
        }

        when (objecte) {
            is Alumno -> {
                document.append("dni", objecte.dni)
                document.append("apenom", objecte.apenom)
                document.append("direc", objecte.direc)
                document.append("pobla", objecte.pobla)
                document.append("telef", objecte.telef)
            }
            is Nota -> {
                document.append("dni", objecte.dni)
                document.append("cod", objecte.cod)
                document.append("nota", objecte.nota)
            }
            is Asignatura -> {
                document.append("cod", objecte.cod)
                document.append("nombre", objecte.nombre)
            }
            else -> throw Exception("Tipo de objeto no soportado: ${objecte::class.simpleName}")
        }

        collection.insertOne(document)
    }

    fun desconnexio() {
        mongoClient?.close() ?: throw Exception("La conneccion no existe")

        //reinicioalizamos los valores
        mongoClient = null
        database = null
    }

}

// Usuatio/cliente  ue utiliza los componentes
//Desenvolupeu una aplicació que fent ús dels components implementats satisfaga l’objectiu d’aquesta pràctica.
// Migrar les dades de la BD school en el SGBD PostgreSQL (pràctica de l’UF2 del mòdul) a equivalent BD en l’entorn del SGBD MongoDB.
fun main() {

    //Connexion Postgres
    val potgres = Potgres()
    val host = "localhost:5432"
    val bd = "school"
    potgres.connexioBD(host = host, bd = bd)

    //Connexion Mongo
    val mongo = Mongo()
    val hostMongo = "migracio-sgbd.oovaypo.mongodb.net"
    val bdMongo = "Migracio-SGBD"
    val user = "AleSmoUser"
    val password = "AleSmoPassword"
    mongo.connexioMongoDb(hostMongo, user, password, bdMongo)

    //Hacemos las migraciones de PostgreSQL a MongoDB
    migrar(potgres, mongo, "alumnos", "Alumnos" )
    migrar(potgres, mongo, "notas", "Notas" )
    migrar(potgres, mongo, "asignaturas", "Asignaturas" )

    potgres.desconnexio()
    mongo.desconnexio()
}

fun migrar(postgres: Potgres, mongo: Mongo, taulaPostgres: String, colleccioMongoDB: String) {
    // Recuperar datos de Alumno de PostgreSQL
    postgres.llegeix(taulaPostgres)

    // Insertar datos de PostgreSQL en MongoDB
    while (postgres.hiha(taulaPostgres)) {
        val registro = postgres.recuperar(taulaPostgres)
        if (registro != null) {
            mongo.insereix(colleccioMongoDB, registro)
        } else {
            println("La tabla no existe")
        }
    }
}

