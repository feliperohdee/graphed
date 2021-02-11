# Graphed

## API
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
			maxEdges?: number,
			minEdges?: number,
			modEdges?: number,
			remoteClosest: function,
			remoteClosestIndex: number
		}): Observable<object>;