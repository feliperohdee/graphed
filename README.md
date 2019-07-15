[![CircleCI](https://circleci.com/gh/feliperohdee/smallorange-redis-edge-graph.svg?style=svg)](https://circleci.com/gh/feliperohdee/smallorange-graph)

# Small Orange Graph

## Node API
		delete({
			id: string
		}): Observable<object>;

		get({
			id: string
		}): Observable<object>;

		multiGet({
			ids: Array<string>
		}): Observable<Array<object>>;

		set({
			id: string,
			data: object
		}): Observable<object>;

		multiSet({
			values: Array<{
				id: string,
				data?: object
			}>,
		}): Observable<Array<object>>;

		update({
			id: string,
			data: object
		}): Observable<object>;

## Edge API
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
			direction?: string
			entity? string,
			fromNode: string,
			toNode: string,
			noInverse?: boolean,
			onlyNodes?: boolean
		}): Observable<object>;

		closest({
			desc?: boolean,
			direction?: string
			distance?: [min?: number, max?: number], // with RedisStore
			entity? string,
			filter? string,
			fromNode: string,
			limit: [min?: number, max?: number]
		}): Observable<object>;

		link({
			absoluteDistance?: number,
			direction?: string
			distance?: number,
			entity? string,
			fromNode: string,
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
		} = require('smallorange-graph');

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
				this.edge = new Edge({
					namespace: 'graphName',
					node: this.node,
					store
				});
			}
		}
