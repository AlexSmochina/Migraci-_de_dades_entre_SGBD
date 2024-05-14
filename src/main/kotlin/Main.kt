import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import org.bson.Document
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.concurrent.timer

// Alumnos
data class Alumno (val dni: String, val apenom: String, val direc: String, val pobla: String, val telef: String)

// Notas
data class Nota (val dni: String, val cod: Int, val nota: Int)

// Asignaturas
data class Asignatura (val cod: Int, val nombre: String)

// COMPONENTES

class Potgres {
    private val baseDatos = ""
    private var connection: Connection? = null
    private val resultSet = mutableMapOf<String,ResultSet>()

    fun connexioBD(host: String, usuari: String? = null, password: String? = null, bd: String){

        val url = "jdbc:postgresql://${host}/${bd}"
        if (usuari == null){
            this.connection = DriverManager.getConnection(url)
        } else {
            this.connection = DriverManager.getConnection(url, usuari, password)
        }

    }

    fun llegeix(taula: String){
        val query = connection?.createStatement() ?: throw Exception("La conneccion no existe")
        val result = query.executeQuery("SELECT * FROM $taula")

        resultSet[taula] = result
    }

    fun hiha(taula: String): Boolean {
        val boolean = resultSet[taula]?.next() ?: false

        return boolean
    }

    fun <Generic>recupera(taula: String): Generic{
        return  recuperar<Generic::>(taula)
    }

    private inline fun <reified Generic> recuperar(taula: String): Generic {
        val rs = resultSet[taula]
        val clazz:Class<Generic> = Generic::class.java

        return when ( clazz ) {
            Nota::class.java -> { recuperarNotas(rs!!) as Generic }
            else -> { throw IllegalArgumentException("Tipo no soportado: ${Generic::class}") }
        }
    }


    private fun recuperarNotas(rs: ResultSet): Nota {
        val nota: Nota = Nota("",0,0)

        return nota
    }

    private fun recuperarAlumno(rs: ResultSet): Alumno {
        val alumno: Alumno

        return alumno
    }

    private fun recuperarAsignatura(rs: ResultSet): Asignatura {
        val asignatura: Asignatura

        return asignatura
    }

    fun desconnexio(taula: String){
        connection?.close() ?: throw Exception("La conneccion no existe")
    }
}

class Mongo {
    private var collection: MongoCollection<Document>? = null

    fun connexioMongoDb(host: String, usuari: String, password: String, bd: String){
        val url = "mongodb+srv://${usuari}:${password}@${host}/?retryWrites=true&w=majority&appName=${bd}"
        val mongoClient = MongoClients.create(url)
        val database = mongoClient.getDatabase(bd)
        collection = database.getCollection("SGBD")
    }


    fun <Generic> insereix(colleccio: String, objecte: Generic){
        val document = Document()

        collection?.insertOne(document)
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
    mongo.connexioMongoDb(hostMongo,user,password,bdMongo)

    //Hacemos las migraciones de PostgreSQL a MongoDB
    migrarAlumnos(potgres,mongo)
    migrarNota(potgres,mongo)
    migrarAsignaturas(potgres,mongo)

}

fun migrarAlumnos(postgres: Potgres, mongo: Mongo) {
    // Recuperar datos de Alumno de PostgreSQL
    postgres.llegeix("alumno")

    // Insertar datos de Alumnos en MongoDB
    while (postgres.hiha("alumno")) {
        val alumno = postgres.recupera<Alumno>("alumno")
        mongo.insereix("Alumnos", alumno)
    }
}

fun migrarNota(postgres: Potgres, mongo: Mongo) {
    // Recuperar datos de Nota de PostgreSQL
    postgres.llegeix("nota")

    // Insertar datos de Nota en MongoDB
    while (postgres.hiha("nota")) {
        val nota = postgres.recupera<Nota>("nota")
        mongo.insereix("Notas", nota)
    }
}

fun migrarAsignaturas(postgres: Potgres, mongo: Mongo) {
    // Recuperar datos de Asignatura de PostgreSQL
    postgres.llegeix("asignatura")

    // Insertar datos de Asignatura en MongoDB
    while (postgres.hiha("asignatura")) {
        val asignatura = postgres.recupera<Asignatura>("asignatura")
        mongo.insereix("Asignaturas", asignatura)
    }
}

