const kafka = require("no-kafka");


/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */

const handler = (eventHandler, errorHandler, consumer) => async (messageSet, topic, partition) => {
    try {
        for (const message of messageSet) {
            await eventHandler(JSON.parse(message.message.value.toString("utf8")))
            await consumer.commitOffset({
                topic,
                partition,
                offset: message.offset,
                metadata: "optional"
            });
        }
    } catch (e) {
        errorHandler(e, consumer);
    }
};

const generateClientID =() => {
    let text = "";
    const possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for (let i = 0; i < 10; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
};

const newConsumer = async (config, eventHandler, errorHandler) => {
    const consumer = new kafka.GroupConsumer(
        {
            connectionString: config.kafka.hosts.join(","),
            groupId: config["consumer-groupV2"],
            clientId: generateClientID()
        }
    );
    const strategies = [{
        subscriptions: [config.kafka.topicV2],
        handler: handler(eventHandler, errorHandler, consumer)
    }];
    return consumer.init(strategies);
};

module.exports = newConsumer;
