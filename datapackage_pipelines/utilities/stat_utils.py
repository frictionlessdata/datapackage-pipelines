STATS_DPP_KEY = '.dpp'
STATS_OUT_DP_URL_KEY = 'out-datapackage-url'


def user_facing_stats(stats):
    if stats is not None and isinstance(stats, dict):
        return dict((k, v) for k, v in stats.items() if k != STATS_DPP_KEY)
    return None
