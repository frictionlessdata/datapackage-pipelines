import yaml
import os

if __name__ == "__main__":
    repos = os.environ.get('DPP_GITHUB_REPOSITORIES')
    if repos is not None:
        repos = repos.split(';')

        config = {}
        for repo in repos:
            repo = repo.split(':')
            if len(repo) > 1:
                repo, path = repo
            else:
                repo = repo[0]
                path = None
            config[repo] = {
                'repository': repo,
            }
            if path is not None:
                config[repo]['base-path'] = path
        with open('github.source-spec.yaml', 'w') as source_spec:
            yaml.dump(config, source_spec)
