import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import org.bson.Document
import java.sql.Connection
import java.sql.DriverManager
import kotlin.concurrent.timer

// Alumnos
class Alumno{
    val dni: String
    val apenom: String
    val direc: String
    val pobla: String
    val telef: String

    constructor( dni: String, apenom: String, direc: String, pobla: String, telef: String){
        this.dni = dni
        this.apenom = apenom
        this.direc = direc
        this.pobla = pobla
        this.telef = telef
    }

    override fun toString(): String {
        return "DNI = $dni ; APENOM = $apenom ; DIREC = $apenom ; DIREC = $direc ; POBLA = $pobla ; TELEF = $telef"
    }
}

// Notas
class Nota{
    val dni: String
    val cod: Int
    val nota: Int

    constructor( dni: String,cod: Int, nota: Int ) {
        this.dni = dni
        this.cod = cod
        this.nota = nota
    }

    override fun toString(): String {
        return "DNI = $dni ; COD = $cod ; NOTA = $nota"
    }
}

// Asignaturas
class Asignaturas {
    val cod: Int
    val nombre: String

    constructor(cod: Int, nombre: String) {
        this.cod = cod
        this.nombre = nombre
    }

    override fun toString(): String {
        return "COD = $cod ; NOMBRE = $nombre"
    }
}

// COMPONENTES

class Potgres {
    private val baseDatos = ""
    private var connection: Connection? = null

    fun connexioBD(host: String, usuari: String? = null, password: String? = null, bd: String){

        val url = "jdbc:postgresql://${host}/${bd}"
        if (usuari == null){
            this.connection = DriverManager.getConnection(url)
        } else {
            this.connection = DriverManager.getConnection(url, usuari, password)
        }

    }

    fun llegeix(taula: String){

    }

    fun hiha(taula: String){

    }

    fun recupera(taula: String){

    }

    fun desconnexio(taula: String){

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
    val connexioBd = potgres.connexioBD(host = host, bd = bd)



    //Connexion Mongo
    val mongo = Mongo()
    val hostMongo = "migracio-sgbd.oovaypo.mongodb.net"
    val bdMongo = "Migracio-SGBD"
    val user = "AleSmoUser"
    val password = "AleSmoPassword"
    val connexioMongo = mongo.connexioMongoDb(hostMongo,user,password,bdMongo)



}

