import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import org.bson.Document

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

fun main() {

    val uri = "mongodb+srv://AleSmoUser:AleSmoPassword@migracio-sgbd.oovaypo.mongodb.net/?retryWrites=true&w=majority&appName=Migracio-SGBD"
    val mongoClient = MongoClients.create(uri)

    val dataBase = mongoClient.getDatabase("Migracio-SGBD")

    val collecion: MongoCollection<Document> = dataBase.getCollection("SGBD")
}

// COMPONENTES

class Potgres {

    fun connexioBD(ost: String, usuari: String, password: String, bd: String){

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

    fun connexioBD(host: String,
                   usuari: String,
                   password: String,
                   bd: String){

    }
}
