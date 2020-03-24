import {ConsoleLogger as Logger, Credentials} from '@aws-amplify/core';
import * as DDB from 'aws-sdk/clients/dynamodb';
import {AnalyticsProvider} from '../types';

const logger = new Logger('AmazonDynamoDBProvider');


export default class AmazonDynamoDBProvider implements AnalyticsProvider {
    private _config;
    private _ddbClient: DDB;

    constructor(config?) {
        this._config = config ? config : {};
    }

    /**
     * Record event
     * @param eventType      - type of the event action. e.g. "Click"
     * @return Promise
     */
    public async record(params, handlers): Promise<boolean> {
    	const credentials = await this._getCredentials();
        if (!credentials) return Promise.resolve(false);
        await this._init();

        const putItemInput: DDB.PutItemInput = {
			Item: params.event.properties, TableName: this._config.tableName
        };

        this._ddbClient.putItem(putItemInput, (err) => {
            if (err) {
                logger.debug("Error in DDB Put", err);
                return handlers.resolve(false);
            }
            return handlers.resolve(false);
        });
        return Promise.resolve(true);
    }

    /**
     * get the category of the plugin
     */
    public getCategory(): string {
        return 'Analytics';
    }

    /**
     * get provider name of the plugin
     */
    public getProviderName(): string {
        return 'AmazonDynamoDB';
    }

    /**
     * configure the plugin
     * @param {Object} config - configuration
     */
    public configure(config): object {
        logger.debug('configure Analytics', config);
        const conf = config ? config : {};
        this._config = Object.assign({}, this._config, conf);
        return this._config;
    }

    /**
     * Initialize the ddb client
     * @private
     * @param params - RequestParams
     */
    private async _init() {
        logger.debug('init clients');
        const credentials = await this._getCredentials();
        if (
            this._ddbClient &&
            this._config.credentials &&
            this._config.credentials.sessionToken === credentials.sessionToken &&
            this._config.credentials.identityId === credentials.identityId
        ) {
            logger.debug('no change for analytics config, directly return from init');
            return true;
        }

        this._config.credentials = credentials;
        const {region} = this._config;
        logger.debug('initialize ddb with credentials', credentials);
        this._ddbClient = new DDB({
            region,
            credentials,
        });
        return true;
    }

    /**
     * check if current credentials exists
     * @private
     */
    private _getCredentials() {
        const that = this;
        return Credentials.get()
            .then(credentials => {
                if (!credentials) return null;
                logger.debug('set credentials for analytics', that._config.credentials);
                return Credentials.shear(credentials);
            })
            .catch(err => {
                logger.debug('ensure credentials error', err);
                return null;
            });
    }
}
