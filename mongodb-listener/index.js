const { MongoClient, ServerApiVersion } = require("mongodb");
const {PubSub} = require('@google-cloud/pubsub');

// Creates a client; cache this for further use

const maxMessages = 10;
const maxWaitTime = 500;

const publishOptions = {
    batching: {
      maxMessages: maxMessages,
      maxMilliseconds: maxWaitTime * 1000,
    },
  };

const pubSubClient = new PubSub();
const topic = pubSubClient.topic('mongodb-changestream', publishOptions)

// Connection URL
const uri = "mongodb://10.29.0.4:27017/";

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri,  {
        serverApi: {
            version: ServerApiVersion.v1,
            strict: true,
            deprecationErrors: true,
        }
    }
);

async function publishMessage(data) {
    // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
    const dataBuffer = Buffer.from(data);
  
    try {
      const messageId = await topic.publishMessage({data: dataBuffer});
      console.log(`Message ${messageId} published.`);
    } catch (error) {
      console.error(`Received error while publishing: ${error.message}`);
      process.exitCode = 1;
    }
  }

async function run() {
  try {
    // Connect the client to the server (optional starting in v4.7)
    await client.connect();

    // Send a ping to confirm a successful connection
    await client.db("admin").command({ ping: 1 });
    console.log("Pinged your deployment. You successfully connected to MongoDB!");

    test_db = client.db("test_db");

    const pipeline = [ { $match: { /*uid: { $in: [ ]}*/}}]
    const changeStream = test_db.watch(pipeline)

    for await (const change of changeStream) {
        console.log("Received change: ", change);
        publishMessage(JSON.stringify(change));
    }

    await changeStream.close();
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
}
run().catch(console.dir);
