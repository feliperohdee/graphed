[![CircleCI](https://circleci.com/gh/feliperohdee/smallorange-redis-edge-graph.svg?style=svg)](https://circleci.com/gh/feliperohdee/smallorange-graph)

# Small Orange Graph

## Node API
		delete({
			node: string
		}): Observable<object>;

		get({
			node: string
		}): Observable<object>;

		multiGet({
			nodes: Array<string>
		}): Observable<Array<object>>;

		set({
			node: string,
			data: object
		}): Observable<object>;

		multiSet({
			values: Array<{
				node: string,
				data?: object
			}>,
		}): Observable<Array<object>>;

		patch({
			node: string,
			data: object
		}): Observable<object>;

## Edge API
		allAll({
			collection: Array<string>,
			direction?: string
			distance?: (collectionSize, fromNodeIndex, toNodeIndex) => number, 
			entity? string,
			noTimestamp?: boolean
		}): Observable<object>;

		count({
			direction?: string
			entity? string,
			fromNode: string,
			max?: number,
			min?: number
		}): Observable<number>;

		delete({
			direction?: string
			entity? string,
			fromNode: string,
			toNode: string
		}): Observable<object>;

		deleteByNode({
			fromNode: string
		}): Observable<object>;

		allByNode({
			by?: 'distance' | 'timestamp',
			direction?: string
			entity? string,
			fromNode: string,
			toNode: string,
			noInverse?: boolean,
			onlyNodes?: boolean
		}): Observable<object>;

		closest({
			by?: 'distance' | 'timestamp',
			desc?: boolean,
			direction?: string
			distance?: [min?: number, max?: number], // with RedisStore
			entity? string,
			filter? string,
			fromNode: string,
			limit: [min?: number, max?: number],
			timestamp: [min?: number, max?: number] // with RedisStore
		}): Observable<object>;

		link({
			absoluteDistance?: number,
			direction?: string
			distance?: number,
			entity? string,
			fromNode: string,
			noTimestamp?: boolean,
			toNode: string
		}): Observable<object>;

		traverse({
			concurrency?: number,
			jobs: Array<{
				absoluteDistance?: number,
				direction?: string
				distance?: number,
				entity? string,
				fromNode: string,
				noTimestamp?: boolean,
				toNode: string
			}>,
			maxPath: number,
			minPath: number,
			remoteClosest: function,
			remoteClosestIndex: number
		}): Observable<object>;

## Sample
		const Redis = require('smallorange-redis-client');
		const {
			Edge,
			Node,
			RedisStore
		} = require('smallorange-redis-edge-graph');

		class App {
			constructor() {
				const store = new RedisStore({
					redis: new Redis({
						connection: {
							port: 6380
						}
					})
				});

				this.store = store;
				this.namespace = 'graph-1';
				
				this.node = new Node({
					namespace: 'graphName',
					store
				});

				this.edge = new Edge({
					namespace: 'graphName',
					node: this.node,
					store
				});
			}
		}
