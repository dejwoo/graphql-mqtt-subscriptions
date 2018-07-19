import { PubSubEngine } from 'graphql-subscriptions/dist/pubsub-engine';
import { connect, Client, ISubscriptionGrant, IClientPublishOptions, IClientSubscribeOptions } from 'mqtt';
import { PubSubAsyncIterator } from './pubsub-async-iterator';
import { QlobberDedup } from 'qlobber';
export interface QlobberWildcardOptions {
    separator?: string,
    wildcard_one?: string,
    wildcard_many?: string,
    cache_adds?: boolean
}

export interface PubSubMQTTOptions {
  brokerUrl?: string;
  client?: Client;
  connectionListener?: (err: Error) => void;
  publishOptions?: PublishOptionsResolver;
  subscribeOptions?: SubscribeOptionsResolver;
  onMQTTSubscribe?: (id: number, granted: ISubscriptionGrant[]) => void;
  triggerTransform?: TriggerTransform;
  parseMessageWithEncoding?: string;
  wildcardOptions?: QlobberWildcardOptions
}

export class MQTTPubSub implements PubSubEngine {

  constructor(options: PubSubMQTTOptions = {}) {
    this.triggerTransform = options.triggerTransform || (trigger => trigger as string);

    if (options.client) {
      this.mqttConnection = options.client;
    } else {
      const brokerUrl = options.brokerUrl || 'mqtt://localhost';
      this.mqttConnection = connect(brokerUrl);
    }

    this.mqttConnection.on('message', this.onMessage.bind(this));

    if (options.connectionListener) {
      this.mqttConnection.on('connect', options.connectionListener);
      this.mqttConnection.on('error', options.connectionListener);
    } else {
      this.mqttConnection.on('error', console.error);
    }

    this.subscriptionMap = {};
    this.subsRefsMap = new QlobberDedup(options.wildcardOptions || {
        separator: '/',
        wildcard_many: '#',
        wildcard_one: '+',
        cache_adds: false
    });
    this.subsIdMap = {};
    this.currentSubscriptionId = 0;
    this.onMQTTSubscribe = options.onMQTTSubscribe || (() => null);
    this.publishOptionsResolver = options.publishOptions || (() => Promise.resolve({}));
    this.subscribeOptionsResolver = options.subscribeOptions || (() => Promise.resolve({}));
    this.parseMessageWithEncoding = options.parseMessageWithEncoding;
  }

  public publish(trigger: string, payload: any): boolean {
    this.publishOptionsResolver(trigger, payload).then(publishOptions => {
      const message = Buffer.from(JSON.stringify(payload), this.parseMessageWithEncoding);

      this.mqttConnection.publish(trigger, message, publishOptions);
    });
    return true;
  }

  public subscribe(trigger: string, onMessage: Function, options?: Object): Promise<number> {
    const triggerName: string = this.triggerTransform(trigger, options);
    const id = this.currentSubscriptionId++;
    this.subscriptionMap[id] = [triggerName, onMessage];
    if (this.subsRefsMap.match(triggerName).size > 0) {
        this.subsRefsMap.add(triggerName, id);
        if (!this.subsIdMap[id]) {
            this.subsIdMap[id] = [];
        }
        this.subsIdMap[id] = [...this.subsIdMap[id], triggerName];
        return Promise.resolve(id);
    } else {
      return new Promise<number>((resolve, reject) => {
        // 1. Resolve options object
        this.subscribeOptionsResolver(trigger, options).then(subscriptionOptions => {

          // 2. Subscribing using MQTT
          this.mqttConnection.subscribe(triggerName, { qos: 0, ...subscriptionOptions }, (err, granted) => {
            if (err) {
              reject(err);
            } else {

              // 3. Saving the new sub id
              this.subsRefsMap.add(triggerName, id);
              if (!this.subsIdMap[id]) {
                this.subsIdMap[id] = [];
              }
              this.subsIdMap[id] = [...this.subsIdMap[id], triggerName];

                // 4. Resolving the subscriptions id to the Subscription Manager
              resolve(id);

              // 5. Notify implementor on the subscriptions ack and QoS
              this.onMQTTSubscribe(id, granted);
            }
          });
        }).catch(err => reject(err));
      });
    }
  }

  public unsubscribe(subId: number) {
    const refs: string[] = this.subsIdMap[subId];

    if (!refs)
      throw new Error(`There is no subscription of id "${subId}"`);

      refs.forEach((triggerName) => {
          this.subsRefsMap.remove(triggerName, subId);
          if (this.subsRefsMap.match(triggerName).size === 0) {
              this.mqttConnection.unsubscribe(triggerName);
          }
      })
    delete this.subsIdMap[subId];
    delete this.subscriptionMap[subId];
  }

  public asyncIterator<T>(triggers: string | string[]): AsyncIterator<T> {
    return new PubSubAsyncIterator<T>(this, triggers);
  }

  private onMessage(topic: string, message: Buffer) {
    const subscribers = this.subsRefsMap.match(topic);
    // Don't work for nothing..
    if (!subscribers || !subscribers.size) {
        return;
    }
    const messageString = message.toString(this.parseMessageWithEncoding);
    let parsedMessage;
    try {
      parsedMessage = JSON.parse(messageString);
    } catch (e) {
      parsedMessage = messageString;
    }
      subscribers.forEach((subId) => {
      const listener = this.subscriptionMap[subId][1];
      listener(parsedMessage);
    });
  }

  private triggerTransform: TriggerTransform;
  private onMQTTSubscribe: SubscribeHandler;
  private subscribeOptionsResolver: SubscribeOptionsResolver;
  private publishOptionsResolver: PublishOptionsResolver;
  private mqttConnection: Client;

  private subscriptionMap: { [subId: number]: [string, Function] };
  private subsRefsMap: any;
  private subsIdMap: {
    [subId:number]: string[]
  }
  private currentSubscriptionId: number;
  private parseMessageWithEncoding: string;
}

export type Path = Array<string | number>;
export type Trigger = string | Path;
export type TriggerTransform = (trigger: Trigger, channelOptions?: Object) => string;
export type SubscribeOptionsResolver = (trigger: Trigger, channelOptions?: Object) => Promise<IClientSubscribeOptions>;
export type PublishOptionsResolver = (trigger: Trigger, payload: any) => Promise<IClientPublishOptions>;
export type SubscribeHandler = (id: number, granted: ISubscriptionGrant[]) => void;
