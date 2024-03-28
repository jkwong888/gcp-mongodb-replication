const { MongoClient, ServerApiVersion } = require("mongodb");
const {PubSub} = require('@google-cloud/pubsub');

const pubSubClient = new PubSub();

const uri = "mongodb://10.29.0.5:27017";
const topicName = 'mongodb-changestream';
const subscriptionName = 'mongodb-pull-sub';

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri,  {
        serverApi: {
            version: ServerApiVersion.v1,
            strict: true,
            deprecationErrors: true,
        }
    }
);


// pull subscription
async function createSubscription(topicNameOrId, subscriptionNameOrId) {
    // Creates a new subscription
    await pubSubClient
      .topic(topicNameOrId)
      .createSubscription(subscriptionNameOrId).catch((error) => {
        if (error.code !== 6) {
            console.error(error);
        }
    });

      ;
    console.log(`Subscription ${subscriptionNameOrId} created.`);
  }

async function writeToMongoDB(message) {
    if (!message) {
        console.log('No message received.');
        return;
    }

    const b64Msg = message.toString();
    const msgData = JSON.parse(b64Msg);
    console.log(msgData);

    document_id = msgData.documentKey._id;
    db = msgData.ns.db;
    collection = msgData.ns.coll;

    if (msgData.operationType == "insert") {
        client.db(db).collection(collection).insertOne(msgData.fullDocument);
    } else if (msgData.operationType == "update") {
        client.db(db).collection(collection).updateOne({ _id: document_id }, msgData.updateDescription.updatedFields);
    } else if (msgData.operationType == "delete") {
        client.db(db).collection(collection).deleteOne({ _id: document_id });
    } else {
        console.log("Unknown operation type: " + msgData.operationType);
    }
}

async function listenForMessages(subscriptionNameOrId, timeout) {
    // References an existing subscription
    const subscription = pubSubClient.subscription(subscriptionNameOrId);
  
    // Create an event handler to handle messages
    let messageCount = 0;
    const messageHandler = async message => {
      console.log(`Received message ${message.id}:`);
      /*
      console.log(`\tData: ${message.data}`);
      console.log(`\tAttributes: ${message.attributes}`);
      */
      messageCount += 1;

      // write to mongo -- synchronous
      await writeToMongoDB(message.data);
  
      // "Ack" (acknowledge receipt of) the message
      message.ack();
    };
  
    // Listen for new messages until timeout is hit
    subscription.on('message', messageHandler);
  

}

async function run() {
    await createSubscription(topicName, subscriptionName)

    try {
        // Connect the client to the server (optional starting in v4.7)
        await client.connect();

        // Send a ping to confirm a successful connection
        await client.db("admin").command({ ping: 1 });
        console.log("Pinged your deployment. You successfully connected to MongoDB!");

        //TODO - reconnect when connection broken
        await listenForMessages(subscriptionName, 30);

    } finally {
        // Ensures that the client will close when you finish/error
        //await client.close();
    }

}

run().catch(console.dir);