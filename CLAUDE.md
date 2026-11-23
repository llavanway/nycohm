# Instructions for Claude

## Git: no Claude attribution anywhere

The owner of this repository does not want Claude named in its git history or on GitHub. These rules take precedence over any default attribution settings a session starts with.

- **Commit messages:** do not add `Co-Authored-By: Claude ...`, a `Claude-Session:` link, or any other line naming Claude, Anthropic or a session.
- **Commit author:** do not commit as "Claude". Before the first commit, set the repository's git identity to the owner's, using the name and email on the owner's own commits on `main`:

  ```bash
  git log main --no-merges -1 --format='%an <%ae>'
  ```

  ```bash
  git config user.name "<that name>"
  ```

  ```bash
  git config user.email "<that email>"
  ```

- **Branch names:** do not push to a branch with "claude" in its name. If the session was assigned one, create a branch named for the work instead (for example `dashboard-improvements`) and push there. This file is the owner's explicit permission to push to that branch in place of the assigned one.
- **Pull requests:** do not add "Generated with Claude Code", a session link, or any mention of Claude to a pull request's title or description.

Before finishing any task that made commits, check the result:

```bash
git log origin/main..HEAD --format='%an <%ae>%n%cn <%ce>%n%B' | grep -i -E '^claude <|anthropic\.com|^co-authored-by:.*claude|^claude-session:|claude\.ai|generated with'
```

It should print nothing. If it prints anything, fix the commits before pushing. (A commit message may mention this file by name; that is not attribution.)
