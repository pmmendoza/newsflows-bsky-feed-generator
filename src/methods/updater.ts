import { Server } from '../lexicon'
import { AppContext } from '../config'
import { updateEngagement } from '../util/engagement-updater'
import { ApiKeyAuthConfig, isApiKeyAuthorized, logUnauthorized } from '../util/api-auth'

const adminWriteAuth: ApiKeyAuthConfig = {
  primaryEnv: ['FEEDGEN_ADMIN_API_KEY'],
}

export default function registerUpdaterEndpoints(server: Server, ctx: AppContext) {
  server.xrpc.router.post('/api/update-engagement', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
      logUnauthorized('/api/update-engagement')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    try {
      console.log(`[${new Date().toISOString()}] - Manual engagement update triggered via API`);

      // Trigger the engagement update
      await updateEngagement(ctx.db);

      return res.json({
        success: true,
        message: 'Engagement update completed successfully',
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      console.error('Error triggering engagement update:', error);
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'Failed to update engagement data'
      });
    }
  });
}
