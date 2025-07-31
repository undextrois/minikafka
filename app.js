//
class MiniKafkaClient 
{
    constructor() 
    {
        this.ws = null;
        this.reconnectAttempts = 0;
    }

    async connect() {
        return new Promise((resolve, reject) => {
            this.ws = new WebSocket('ws://localhost:8765');
            
            this.ws.onopen = () => {
                this.reconnectAttempts = 0;
                resolve();
            };
            
            this.ws.onclose = (e) => {
                if (e.code === 1011) {
                    reject(new Error("Backend unavailable"));
                } else {
                    this.attemptReconnect();
                }
            };
            
            this.ws.onerror = (error) => {
                reject(error);
            };
        });
    }

    attemptReconnect() {
        if (this.reconnectAttempts++ < 3) {
            setTimeout(() => this.connect(), 1000 * this.reconnectAttempts);
        }
    }

    async produce(topic, value) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error("WebSocket is not connected");
        }
        
        const message = {
            action: "produce",
            topic: topic,
            value: value
        };
        this.ws.send(JSON.stringify(message));
    }


    disconnect() {
        if (this.socket) {
            this.socket.close();
            this.socket = null;
            this.connected = false;
        }
    }

    sendCommand(command) {
        return new Promise((resolve, reject) => {
            if (!this.connected || !this.socket) {
                reject(new Error("Not connected to broker"));
                return;
            }

            const messageId = Date.now().toString();
            const messageWithId = { ...command, messageId };

            const handleResponse = (event) => {
                try {
                    const response = JSON.parse(event.data);
                    if (response.messageId === messageId) {
                        this.socket.removeEventListener('message', handleResponse);
                        if (response.status === "error") {
                            reject(new Error(response.message));
                        } else {
                            resolve(response);
                        }
                    }
                } catch (e) {
                    console.error("Error processing response:", e);
                }
            };

            this.socket.addEventListener('message', handleResponse);
            this.socket.send(JSON.stringify(messageWithId));
        });
    }

    createTopic(name, partitions = 1) {
        return this.sendCommand({
            action: "create_topic",
            topic: name,
            partitions: partitions
        });
    }

    listTopics() {
        return this.sendCommand({
            action: "list_topics"
        });
    }

    produce(topic, value, key = null) {
        return this.sendCommand({
            action: "produce",
            topic: topic,
            value: value,
            key: key
        });
    }

    consume(topic, partition = 0, offset = 0, maxMessages = 100) {
        return this.sendCommand({
            action: "consume",
            topic: topic,
            partition: partition,
            offset: offset,
            max_messages: maxMessages
        });
    }

    subscribe(topic, partition = 0, callback) {
        const callbackKey = `${topic}:${partition}`;
        this.subscriptionCallbacks[callbackKey] = callback;

        return this.sendCommand({
            action: "subscribe",
            topic: topic,
            partition: partition
        });
    }

    getMetrics() {
        return this.sendCommand({
            action: "get_metrics"
        });
    }
}

// UI Application
class KafkaWebClient {
    constructor() {
        this.client = null;
        this.currentSubscription = null;
        this.initElements();
        this.setupEventListeners();
        this.updateConnectionStatus(false);
    }

    initElements() {
        this.elements = {
            brokerUrl: document.getElementById('broker-url'),
            connectBtn: document.getElementById('connect-btn'),
            connectionStatus: document.getElementById('connection-status'),
            
            newTopic: document.getElementById('new-topic'),
            partitions: document.getElementById('partitions'),
            createTopicBtn: document.getElementById('create-topic-btn'),
            topicsList: document.getElementById('topics-list'),
            
            produceTopic: document.getElementById('produce-topic'),
            messageKey: document.getElementById('message-key'),
            messageValue: document.getElementById('message-value'),
            produceBtn: document.getElementById('produce-btn'),
            produceResult: document.getElementById('produce-result'),
            
            consumeTopic: document.getElementById('consume-topic'),
            partition: document.getElementById('partition'),
            offset: document.getElementById('offset'),
            consumeBtn: document.getElementById('consume-btn'),
            subscribeBtn: document.getElementById('subscribe-btn'),
            messagesList: document.getElementById('messages-list')
        };
    }

    setupEventListeners() {
        this.elements.connectBtn.addEventListener('click', () => this.toggleConnection());
        
        this.elements.createTopicBtn.addEventListener('click', () => this.createTopic());
        this.elements.produceBtn.addEventListener('click', () => this.produceMessage());
        this.elements.consumeBtn.addEventListener('click', () => this.consumeMessages());
        this.elements.subscribeBtn.addEventListener('click', () => this.toggleSubscription());
        
        // Replace DOMSubtreeModified with MutationObserver
        const topicsObserver = new MutationObserver((mutations) => {
            this.populateTopicDropdowns();
        });
    
        topicsObserver.observe(this.elements.topicsList, {
            childList: true,       // Observe direct children additions/removals
            subtree: true,         // Observe all descendants
            characterData: false   // Don't observe text changes
        });
    
        // Store observer for potential disconnection later
        this.topicsObserver = topicsObserver;
    }

    async toggleConnection() {
        if (this.client && this.client.connected) {
            this.client.disconnect();
            this.updateConnectionStatus(false);
            this.elements.connectBtn.textContent = "Connect";
        } else {
            try {
                const brokerUrl = this.elements.brokerUrl.value;
                this.client = new MiniKafkaClient(brokerUrl);
                await this.client.connect();
                this.updateConnectionStatus(true);
                this.elements.connectBtn.textContent = "Disconnect";
                this.loadTopics();
            } catch (error) {
                this.showError(`Connection failed: ${error.message}`);
                this.updateConnectionStatus(false);
            }
        }
    }

    updateConnectionStatus(connected) {
        const statusElement = this.elements.connectionStatus;
        statusElement.textContent = connected ? "Connected" : "Disconnected";
        statusElement.className = connected ? "status-connected" : "status-disconnected";
        
        // Enable/disable UI elements based on connection status
        const interactiveElements = [
            this.elements.createTopicBtn,
            this.elements.produceBtn,
            this.elements.consumeBtn,
            this.elements.subscribeBtn
        ];
        
        interactiveElements.forEach(el => {
            el.disabled = !connected;
        });
    }

    async createTopic() {
        const topicName = this.elements.newTopic.value.trim();
        const partitions = parseInt(this.elements.partitions.value);
        
        if (!topicName) {
            this.showError("Please enter a topic name");
            return;
        }
        
        try {
            await this.client.createTopic(topicName, partitions);
            this.elements.newTopic.value = "";
            this.showSuccess(`Topic "${topicName}" created successfully`);
            this.loadTopics();
        } catch (error) {
            this.showError(`Failed to create topic: ${error.message}`);
        }
    }

    async loadTopics() {
        try {
            const response = await this.client.listTopics();
            this.displayTopics(response.topics);
        } catch (error) {
            this.showError(`Failed to load topics: ${error.message}`);
        }
    }

    displayTopics(topics) {
        this.elements.topicsList.innerHTML = "";
        
        if (!topics || topics.length === 0) {
            this.elements.topicsList.innerHTML = "<li>No topics found</li>";
            return;
        }
        
        topics.forEach(topic => {
            const li = document.createElement('li');
            li.innerHTML = `
                <strong>${topic.name}</strong>
                <span class="topic-meta">Partitions: ${topic.partitions}</span>
            `;
            this.elements.topicsList.appendChild(li);
        });
    }

    populateTopicDropdowns() {
        const topicSelects = [this.elements.produceTopic, this.elements.consumeTopic];
        
        topicSelects.forEach(select => {
            const currentValue = select.value;
            select.innerHTML = "";
            
            const topics = Array.from(this.elements.topicsList.querySelectorAll('li strong')).map(el => el.textContent);
            
            if (topics.length === 0) {
                const option = document.createElement('option');
                option.value = "";
                option.textContent = "No topics available";
                select.appendChild(option);
                select.disabled = true;
                return;
            }
            
            select.disabled = false;
            
            topics.forEach(topic => {
                const option = document.createElement('option');
                option.value = topic;
                option.textContent = topic;
                select.appendChild(option);
            });
            
            if (topics.includes(currentValue)) {
                select.value = currentValue;
            }
        });
    }

    async produceMessage() {
        const topic = this.elements.produceTopic.value;
        const key = this.elements.messageKey.value.trim();
        const value = this.elements.messageValue.value.trim();
        
        if (!topic) {
            this.showError("Please select a topic");
            return;
        }
        
        if (!value) {
            this.showError("Please enter a message value");
            return;
        }
        
        try {
            const response = await this.client.produce(topic, value, key || null);
            this.showSuccess(`Message produced to ${topic}:${response.partition} at offset ${response.offset}`);
            this.elements.messageValue.value = "";
        } catch (error) {
            this.showError(`Failed to produce message: ${error.message}`);
        }
    }

    async consumeMessages() {
        const topic = this.elements.consumeTopic.value;
        const partition = parseInt(this.elements.partition.value);
        const offset = parseInt(this.elements.offset.value);
        
        if (!topic) {
            this.showError("Please select a topic");
            return;
        }
        
        try {
            const response = await this.client.consume(topic, partition, offset);
            this.displayMessages(response.messages);
        } catch (error) {
            this.showError(`Failed to consume messages: ${error.message}`);
        }
    }

    displayMessages(messages) {
        this.elements.messagesList.innerHTML = "";
        
        if (!messages || messages.length === 0) {
            this.elements.messagesList.innerHTML = "<div class='message-item'>No messages found</div>";
            return;
        }
        
        messages.forEach(msg => {
            const messageDiv = document.createElement('div');
            messageDiv.className = "message-item";
            messageDiv.innerHTML = `
                <div class="message-header">Message at offset ${msg.offset}</div>
                <div class="message-meta">
                    Partition: ${msg.partition} | 
                    Key: ${msg.key || '(none)'} | 
                    ${new Date(msg.timestamp).toLocaleString()}
                </div>
                <div class="message-value">${msg.value}</div>
            `;
            this.elements.messagesList.appendChild(messageDiv);
        });
    }

    async toggleSubscription() {
        if (this.currentSubscription) {
            this.unsubscribe();
            this.elements.subscribeBtn.textContent = "Subscribe";
        } else {
            await this.subscribe();
            this.elements.subscribeBtn.textContent = "Unsubscribe";
        }
    }

    async subscribe() {
        const topic = this.elements.consumeTopic.value;
        const partition = parseInt(this.elements.partition.value);
        
        if (!topic) {
            this.showError("Please select a topic");
            return false;
        }
        
        try {
            this.currentSubscription = {
                topic,
                partition,
                callback: (message) => {
                    this.displayMessages([message]);
                }
            };
            
            await this.client.subscribe(topic, partition, this.currentSubscription.callback);
            return true;
        } catch (error) {
            this.showError(`Failed to subscribe: ${error.message}`);
            this.currentSubscription = null;
            return false;
        }
    }

    unsubscribe() {
        if (this.currentSubscription) {
            // In a real implementation, we would send an unsubscribe command to the server
            this.currentSubscription = null;
        }
    }

    showSuccess(message) {
        this.elements.produceResult.textContent = message;
        this.elements.produceResult.className = "success";
    }

    showError(message) {
        this.elements.produceResult.textContent = message;
        this.elements.produceResult.className = "error";
    }
}

// Initialize the application when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    const app = new KafkaWebClient();
});