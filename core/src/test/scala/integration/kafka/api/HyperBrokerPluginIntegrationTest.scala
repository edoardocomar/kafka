package integration.kafka.api

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaApis
import kafka.utils.Logging
import kafka.utils.TestUtils.{waitUntilBrokerMetadataIsPropagated, waitUntilTrue}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, CreateTopicsRequest, CreateTopicsResponse, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.HyperBrokerPlugin
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._
import org.opentest4j.AssertionFailedError

import java.net.InetAddress
import java.util
import scala.jdk.CollectionConverters._
import java.util.{Optional, Properties}
import java.util.concurrent.ExecutionException
import scala.collection.Seq

// HyperPlugin specific
class HyperBrokerPluginIntegrationTest extends IntegrationTestHarness with Logging {
  def brokerCount = 3
  var client: Admin = _
  var testInfo : TestInfo = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    this.testInfo = testInfo
    if ("testInterceptRequestResponse()".equals(testInfo.getDisplayName)) {
      serverConfig.put(HyperBrokerPlugin.PROP_NAME, classOf[TestRequestResponsePlugin].getName)
    }
    if ("testBypassAPI()".equals(testInfo.getDisplayName)) {
      serverConfig.put(HyperBrokerPlugin.PROP_NAME, classOf[TestBypassApiPlugin].getName)
    }

    super.setUp(testInfo)
    waitUntilBrokerMetadataIsPropagated(brokers)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()

    if ("testInterceptRequestResponse()".equals(testInfo.getDisplayName)) {
      // test the lifecycle of plugin instances
      assertEquals(brokerCount, TestRequestResponsePlugin.getInstances.length)
      TestRequestResponsePlugin.getInstances.foreach(ti => {
        assertEquals(1, ti.closeCalled, "close not called in plugin " + ti.brokerId)
      })
    }
  }

  @Test
  def testInterceptRequestResponse(): Unit = {
    client = createAdminClient
    val newTopics = Seq(new NewTopic("mytopic", 1, 3.toShort))
    val validateResult = client.createTopics(newTopics.asJava)
    validateResult.all.get()

    // test that created topic has been renamed as per plugin implementation
    val expectedTopics = Seq("test-mytopic")
    waitForTopics(client, expectedTopics)

    // test the APIs of plugin instances have been invoked
    assertEquals(brokerCount, TestRequestResponsePlugin.getInstances.length)
    var numOfNotNullIPs = 0
    TestRequestResponsePlugin.getInstances.foreach(ti => {
      assertNotNull(ti.kafkaApis, "null kafkaApis")
      assertNotNull(ti.brokerId, "null brokerId")
      if (ti.lastClientAddress != null) {
        numOfNotNullIPs += 1
      }
    })
    assertTrue(numOfNotNullIPs > 0, "num of numOfNotNullIPs must be > 0")
  }

  @Test
  def testBypassAPI(): Unit = {
    client = createAdminClient
    val newTopics = Seq(new NewTopic("mytopic", 1, 3.toShort))
    val validateResult = client.createTopics(newTopics.asJava)
    validateResult.all.get()
    waitForTopics(client, Seq("mytopic"))

    val newZTopics = Seq(new NewTopic("Zmytopic", 1, 3.toShort))
    val validateZResult = client.createTopics(newZTopics.asJava)

    try {
      validateZResult.all.get()
      fail("PolicyViolationException expected")
    } catch {
      case e : ExecutionException => assertTrue(e.getCause.isInstanceOf[PolicyViolationException]) // ok
      case e : AssertionFailedError => throw e
      case e : Throwable => fail("PolicyViolationException expected, got " + e)
    }
  }

  def createAdminClient: Admin = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    val client = createAdminClient(configOverrides = props)
    client
  }

  def waitForTopics(client: Admin, expectedPresent: Seq[String]): Unit = {
    waitUntilTrue(() => {
      val topics = client.listTopics.names.get()
      expectedPresent.forall(topicName => topics.contains(topicName))
    }, "timed out waiting for topics")
  }
}

class TestRequestResponsePlugin extends HyperBrokerPlugin {
  // add each instance to the registry of all TestPlugin instances
  TestRequestResponsePlugin.addInstance(this)

  var kafkaApis : KafkaApis = _
  var lastClientAddress: InetAddress = _
  var closeCalled: Int = _
  var brokerId: String = _

  override def interceptRequest[T <: AbstractRequest](principal: KafkaPrincipal, requestHeader: RequestHeader, requestBody: T): T = {
    if (requestHeader.apiKey().id == ApiKeys.CREATE_TOPICS.id) {
      val data = requestBody.asInstanceOf[CreateTopicsRequest].data
      data.topics().forEach( ct => {
        if ("mytopic".equals(ct.name())) {
          ct.setName("test-" + ct.name())
        }
      })
    }
    requestBody
  }

  override def interceptResponse[T <: AbstractResponse](principal: KafkaPrincipal, requestHeader: RequestHeader, responseBody: T): T = {
    if (requestHeader.apiKey().id == ApiKeys.CREATE_TOPICS.id) {
      val data = responseBody.asInstanceOf[CreateTopicsResponse].data
      data.topics().forEach( ct => {
        if ("test-mytopic".equals(ct.name())) {
          ct.setName("mytopic")
        }
      })
    }
    responseBody
  }

  override def interceptClientAddress(principal: KafkaPrincipal, clientAddress: InetAddress): InetAddress = {
    lastClientAddress = clientAddress
    clientAddress
  }

  override def setKafkaApis(kafkaApis: Any): Unit = {
    this.kafkaApis = kafkaApis.asInstanceOf[KafkaApis]
  }

  override def close(): Unit = {
    closeCalled = closeCalled + 1
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    this.brokerId = configs.get(ServerConfigs.BROKER_ID_CONFIG).asInstanceOf[String]
  }
}

// a registry of all created TestPlugin instances
object TestRequestResponsePlugin {
  private var instances: List[TestRequestResponsePlugin] = List.empty

  def addInstance(instance: TestRequestResponsePlugin): Unit = {
    instances = instance :: instances
  }

  def getInstances: List[TestRequestResponsePlugin] = instances
}

class TestBypassApiPlugin extends HyperBrokerPlugin {
  var kafkaApis: KafkaApis = _

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def setKafkaApis(kafkaApis: Any): Unit = {
    this.kafkaApis = kafkaApis.asInstanceOf[KafkaApis]
  }

  override def bypassApi[T <: AbstractRequest, S <: AbstractResponse](principal: KafkaPrincipal, requestHeader: RequestHeader, requestBody: T): Optional[S] = {
    if (requestHeader.apiKey().id == ApiKeys.CREATE_TOPICS.id) {
      val data = requestBody.asInstanceOf[CreateTopicsRequest].data
      data.topics().forEach( ct => {
        if (ct.name().toLowerCase.startsWith("z")) {
          val err = new PolicyViolationException("Illegal topic name - starts with z")
          return Optional.of(requestBody.getErrorResponse(err).asInstanceOf[S])
        }
      })
    }
    super.bypassApi(principal, requestHeader, requestBody)
  }
}
