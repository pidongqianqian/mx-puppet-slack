import { IDbSchema, Store } from "@pidong/mx-puppet-bridge";

const LEVEL_OP = 100;

export class Schema implements IDbSchema {
	public description = "Record user's synchronized team and channel";
	public async run(store: Store) {
		await store.createTable(`
			CREATE TABLE user_team_channel (
				channel_id VARCHAR(255) NOT NULL,
				team_id VARCHAR(255) NOT NULL,
				user_id VARCHAR(255) NOT NULL,
				room_id VARCHAR(255) NOT NULL,
				PRIMARY KEY (channel_id, team_id, user_id, room_id)
			);`, "user_team_channel");
	}
	public async rollBack(store: Store) {
		await store.db.Exec("DROP TABLE IF EXISTS user_team_channel");
	}
}
