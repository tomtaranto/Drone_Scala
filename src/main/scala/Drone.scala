import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.Formats
import play.api.libs.json.OWrites

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.math.floor
import scala.util.Random
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import play.api.libs.json.Json

import scala.math.abs


case class Rapport2(id_drone : Int, ville: String, list_id: List[Int], list_nom: List[String], list_prenom: List[String], timestamp : String, list_positivite : List[Int], battery : Long){}

class Drone(val id: Int, val id_ville: Int){

  val list_id : List[Int] = List.range(4000 * id_ville , 4000 * (id_ville + 1))
  val list_ville : List[String] = List("Paris", "Londres", "Madrid", "Bruxelles")
  val ville : String = list_ville(id_ville)
  val init_time : Int = (System.currentTimeMillis() / 1000).toInt
  val init_battery : Int = init_time % 100 +  90 + Random.nextInt(11) // Un chiffre aléatoire entre 90 et 190
  val battery_decay: Double = Random.nextDouble()/10 + 0.9 // Un chiffre entre 0.9 et 1.0

  val TOPIC="peaceland"
  val props: Properties = new Properties()

  props.put("bootstrap.servers", "127.0.0.1:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer : KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  /* Fonction qui genere un nom a partir d'un identifiant entier */
  def generate_nom_from_id(i:Int) : String = {
    Random.setSeed(i)
    Random.alphanumeric.filter(_.isLetter).take(15).mkString("").toLowerCase
  }

  def generate_prenom_from_id(i:Int) : String = {
    Random.setSeed(i+1)
    Random.alphanumeric.filter(_.isLetter).take(15).mkString("").toLowerCase
  }

  def generate_positivite(i:Int) : Int = {
    floor(Random.nextFloat() *100.0).toInt
  }

  def generate_positivite_liste() : List[Int] = {
    Random.setSeed(System.currentTimeMillis() * 1000)
    this.list_id.map(i=>generate_positivite(i))
  }

  val list_nom : List[String] = list_id.map(i=>generate_nom_from_id(i))
  val list_prenom : List[String] = list_id.map(i=>generate_prenom_from_id(i))

  //TODO
  // changer la fonction de choix random par un choix fixé par un fichier de config
  /* Fonction qui choisit i entiers dans la list_id */
  def pick_random_ids(i:Int) : List[Int] = {
    val res : List[Int] = Random.shuffle(List.range(0, list_id.size)).take(i)
    res
  }

  def find_by_id(l1 : List[Int], l2 : List[String]) : List[String] = {
    l1.map(l2)
  }

  @deprecated
  def show_id() : Int = {
    println(id)
    id
  }

  //TODO
  // Generer un rapport a l'aide des informations du done
  def generate_and_send_rapport(list_bad_id : List[Int]) : Unit = {
    val esapsed_millis = 1623090624925L

    val custom_timestamp = new SimpleDateFormat("dd/MM/yyyy hh:mm aa").format(new Date(esapsed_millis + Random.nextInt(scala.math.pow(10,12).toInt)))

    //val custom_timestamp = new SimpleDateFormat("dd/MM/yyyy hh:mm aa").format(new Date(esapsed_millis + Random.nextInt(365) * scala.math.pow(2,7778,-7)))
    val current_battery: Long = ((battery_decay / (abs(System.currentTimeMillis()/1000).toInt - init_time)) * init_battery).toLong
    println("battery actuelle",current_battery,"vs",custom_timestamp,battery_decay)
    if (current_battery > 5){
      //val rapport = new Rapport(id, ville, list_id, find_by_id(list_id, list_nom), find_by_id(list_id, list_prenom),custom_timestamp, this.generate_positivite_liste())
      implicit val RapportWrite: OWrites[Rapport2] = Json.writes[Rapport2]
      val list_positivite = list_bad_id.map(i=>generate_positivite(i))
      val json_rapport = Json.toJson(Rapport2(id, ville, list_bad_id.map(list_id), find_by_id(list_bad_id, list_nom), find_by_id(list_bad_id, list_prenom), custom_timestamp, list_positivite, current_battery))
      println(Json.prettyPrint(json_rapport))
      println(custom_timestamp)
      //println(Json.stringify(json_rapport))

      implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
      //val record = new ProducerRecord(TOPIC, "key", write(rapport.generate()))
      val record = new ProducerRecord(TOPIC, "key", write(Json.stringify(json_rapport)))
      //println("message envoyé")
      producer.send(record)
    }
  }

  def close_producer(): Unit = {
    producer.close()
  }
}


object Flotte extends App{
  val list_ids_drone = (0 to 100).toList
  val list_id_ville = (0 to 3).toList
  val list_drones = list_ids_drone.map(drone_id => new Drone(drone_id, drone_id%4))
  println(list_ids_drone)
  println(list_id_ville)
  println(list_drones)
  val fake_loop = (0 to 20).toList
  fake_loop.foreach{ i =>
    println(i)
    list_drones.foreach(drone=> drone.generate_and_send_rapport(drone.pick_random_ids(Random.nextInt(20))))
    Thread.sleep(1700)
  }
  list_drones.foreach(drone => drone.close_producer())
}
