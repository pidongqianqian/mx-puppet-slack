import { Store } from "@pidong/mx-puppet-bridge";
import { IStoreToken } from "soru-slack-client";

const CURRENT_SCHEMA = 2;

type userTeamChannel = {
	userId: string,
	teamId: string,
	channelId: string,
	roomId: string,
	puppetId: number
}
type userTeamChannels = userTeamChannel[];

export class SlackStore {
	constructor(
		private store: Store,
	) { }

	public async init(): Promise<void> {
		await this.store.init(CURRENT_SCHEMA, "slack_schema", (version: number) => {
			return require(`./db/schema/v${version}.js`).Schema;
		}, false);
	}

	public async getTokens(puppetId: number): Promise<IStoreToken[]> {
		const rows = await this.store.db.All("SELECT * FROM slack_tokenstore WHERE puppet_id = $p", { p: puppetId });
		const ret: IStoreToken[] = [];
		for (const row of rows) {
			if (row) {
				ret.push({
					token: row.token as string,
					teamId: row.team_id as string,
					userId: row.user_id as string,
				});
			}
		}
		return ret;
	}

	public async storeToken(puppetId: number, token: IStoreToken) {
		const exists = await this.store.db.Get("SELECT 1 FROM slack_tokenstore WHERE puppet_id = $p AND token = $t",
			{ p: puppetId, t: token.token });
		if (exists) {
			return;
		}
		await this.store.db.Run(`INSERT INTO slack_tokenstore (
			puppet_id, token, team_id, user_id
		) VALUES (
			$puppetId, $token, $teamId, $userId
		)`, {
			puppetId,
			token: token.token,
			teamId: token.teamId,
			userId: token.userId,
		});
	}
	
	public async storeUserChannels(userTeamChannels: userTeamChannels) {
		if (this.store.db.type === "postgres") {
			await this.store.db.BulkInsert(`INSERT INTO user_team_channel (
				channel_id, team_id, user_id, room_id, puppet_id
			) VALUES (
				$channelId, $teamId, $userId, $roomId. $puppetId
			) ON CONFLICT DO NOTHING;`, userTeamChannels);
		} else {
			await this.store.db.BulkInsert(`INSERT OR IGNORE INTO user_team_channel (
				channel_id, team_id, user_id, room_id, puppet_id
			) VALUES (
				$channelId, $teamId, $userId, $roomId, $puppetId
			)`, userTeamChannels);
		}
	}

	public async getTeamRooms(teamId: string, userId: string) {
		const rows = await this.store.db.All("SELECT * FROM user_team_channel WHERE team_id = $t AND user_id = $u",
			{ t: teamId, u: userId });
		const ret: userTeamChannel[] = [];
		for (const row of rows) {
			if (row) {
				ret.push({
					roomId: row.room_id as string,
					userId: row.user_id as string,
					channelId: row.channel_id as string,
					teamId: row.team_id as string,
					puppetId: row.puppet_id as number
				});
			}
		}
		return ret;
	}

	public async getRoomByRoomIdAndUserId(roomId: string, userId: string) {
		const rows = await this.store.db.All("SELECT * FROM user_team_channel WHERE room_id = $t AND user_id = $u",
			{ t: roomId, u: userId });
		const ret: userTeamChannel[] = [];
		for (const row of rows) {
			if (row) {
				ret.push({
					roomId: row.room_id as string,
					userId: row.user_id as string,
					channelId: row.channel_id as string,
					teamId: row.team_id as string,
					puppetId: row.puppet_id as number,
				});
			}
		}
		return ret;
	}

	public async getRoomByChannelIdAndTeamId(channelId: string, teamId: string) {
		const rows = await this.store.db.All("SELECT * FROM user_team_channel WHERE channel_id = $t AND team_id = $u",
			{ t: channelId, u: teamId });
		const ret: userTeamChannel[] = [];
		for (const row of rows) {
			if (row) {
				ret.push({
					roomId: row.room_id as string,
					userId: row.user_id as string,
					channelId: row.channel_id as string,
					teamId: row.team_id as string,
					puppetId: row.puppet_id as number,
				});
			}
		}
		return ret;
	}

	public async deleteTeamRooms(teamId: string, userId: string) {
		await this.store.db.Run("DELETE FROM user_team_channel WHERE team_id = $t AND user_id = $u",
			{ t: teamId, u: userId });
	}
}
