# ruff: noqa

"""
Bitbucket API Usage Examples (OAuth + Basic Auth)

This example demonstrates how to use the Bitbucket DataSource to interact with
the Bitbucket Cloud API (v2.0), covering:
- Authentication (OAuth 2.0 flow or Basic Auth)
- Initializing the Client and DataSource
- Fetching User Details
- Listing Workspaces and Repositories

Prerequisites for OAuth:
1. Create an OAuth consumer in Bitbucket (Settings -> OAuth consumers).
2. Set Callback URL to http://localhost:8080/callback
3. Set environment variables: BITBUCKET_CLIENT_ID, BITBUCKET_CLIENT_SECRET

Prerequisites for Basic Auth:
1. Log in to Bitbucket.
2. Go to Personal Settings -> App Passwords.
3. Create an App Password with permissions (Read: Account, Workspace, Repositories, Pull Requests, Issues).
4. Set environment variables: BITBUCKET_USERNAME, BITBUCKET_PASSWORD
"""

import asyncio
import json
import os
import secrets
import threading
import time
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from app.sources.client.bitbucket.bitbucket import (
    BitbucketBasicAuthConfig,
    BitbucketClient,
    BitbucketOAuthConfig,
    BitbucketTokenConfig,
)
from app.sources.external.bitbucket.bitbucket import (
    BitbucketDataSource,
    BitbucketResponse,
)

# --- Configuration ---
USERNAME = os.getenv("BITBUCKET_USERNAME")
PASSWORD = os.getenv("BITBUCKET_PASSWORD")
TOKEN = os.getenv("BITBUCKET_TOKEN")

# OAuth Configuration
CLIENT_ID = os.getenv("BITBUCKET_CLIENT_ID")
CLIENT_SECRET = os.getenv("BITBUCKET_CLIENT_SECRET")
REDIRECT_URI = os.getenv("BITBUCKET_REDIRECT_URI", "http://localhost:8080/callback")
BASE_URL = os.getenv("BITBUCKET_BASE_URL", "https://api.bitbucket.org/2.0")

# Globals for OAuth flow
auth_code = None

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP request handler for OAuth callback"""

    def do_GET(self):
        """Handle GET request to callback URL"""
        global auth_code
        query_components = parse_qs(urlparse(self.path).query)
        if 'code' in query_components:
            auth_code = query_components['code'][0]
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"""
            <html>
                <body style="font-family: sans-serif; text-align: center; padding: 50px;">
                    <h1 style="color: green;">Authorization Successful!</h1>
                    <p>You can close this window and return to the terminal.</p>
                </body>
            </html>
            """)
        else:
            self.send_response(400)
            self.wfile.write(b"Authorization Failed: No code found.")

    def log_message(self, format, *args):
        pass  # Suppress logging


def start_callback_server(port=8080):
    """Start local HTTP server for OAuth callback - handles one request"""
    server = HTTPServer(('localhost', port), OAuthCallbackHandler)
    
    def serve_once():
        server.handle_request()
    
    thread = threading.Thread(target=serve_once)
    thread.daemon = True
    thread.start()
    return server


def print_section(title: str):
    print(f"\n{'-'*80}")
    print(f"| {title}")
    print(f"{'-'*80}")


def print_result(name: str, response: BitbucketResponse, show_data: bool = True):
    if response.success:
        print(f"✅ {name}: Success")
        if show_data and response.data:
            data_to_show = response.data
            if isinstance(data_to_show, dict) and "values" in data_to_show:
                items = data_to_show["values"]
                print(f"   Found {len(items)} item(s) in current page.")
                if len(items) > 0:
                    print(f"   Sample item: {json.dumps(items[0], indent=2)[:300]}...")
            else:
                print(f"   Data: {json.dumps(data_to_show, indent=2)[:500]}...")
    else:
        print(f"❌ {name}: Failed")
        if response.error:
            print(f"   Error: {response.error}")
        if response.message:
            print(f"   Message: {response.message}")
        if response.data:
            print(f"   Response data: {json.dumps(response.data, indent=2)[:500]}...")
        if not response.error and not response.message:
            print(f"   No error details available. Check authentication token/credentials.")


async def main() -> None:
    print_section("Initializing Bitbucket Client")
    config = None
    client = None

    # Debug: Show what credentials are available
    has_oauth = bool(CLIENT_ID and CLIENT_SECRET)
    has_token = bool(TOKEN)
    has_basic = bool(USERNAME and PASSWORD)
    
    if has_oauth:
        print(f"   Found OAuth credentials (CLIENT_ID: {CLIENT_ID[:10]}...)")
    if has_token:
        print(f"   Found Token (will be used if OAuth not available)")
    if has_basic:
        print(f"   Found Basic Auth credentials (will be used if OAuth/Token not available)")

    # 1. Try OAuth if keys are present (prioritize OAuth over token)
    if CLIENT_ID and CLIENT_SECRET:
        print("ℹ️  OAuth credentials found. Starting OAuth flow...")

        # Initialize OAuth configuration
        oauth_config = BitbucketOAuthConfig(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri=REDIRECT_URI,
            base_url=BASE_URL
        )

        # Create OAuth client directly
        oauth_client = oauth_config.create_client()

        # Start callback server
        print(f"   Starting callback server at {REDIRECT_URI}...")
        start_callback_server(8080)
        time.sleep(0.5)  # Give server a moment to start

        # Get authorization URL
        state = secrets.token_urlsafe(32)
        auth_url = oauth_client.get_authorization_url(state)

        print(f"   Opening browser: {auth_url}")
        webbrowser.open(auth_url)

        # Wait for authorization code
        print("   Waiting for authorization code...")
        timeout = 60
        start_time = time.time()
        while auth_code is None and (time.time() - start_time) < timeout:
            time.sleep(0.5)

        if not auth_code:
            print("❌ Timeout waiting for auth code.")
            print("   Please make sure you authorize the application in the browser.")
            return

        print("   Code received. Exchanging for token...")

        # Exchange code for token
        try:
            access_token = await oauth_client.initiate_oauth_flow(auth_code)
            if not access_token:
                print("❌ Failed to obtain access token.")
                return

            print(f"✅ OAuth Success! Token obtained: {access_token[:10]}...")

            # Create token config and build client
            config = BitbucketTokenConfig(token=access_token, base_url=BASE_URL)
            client = BitbucketClient.build_with_config(config)

        except Exception as e:
            print(f"❌ OAuth Exchange failed: {e}")
            return

    # 2. Fallback to Basic Auth/Token (only if OAuth not attempted)
    elif TOKEN:
        print("ℹ️  Using Bearer Token authentication")
        config = BitbucketTokenConfig(token=TOKEN, base_url=BASE_URL)
        client = BitbucketClient.build_with_config(config)
    elif USERNAME and PASSWORD:
        print("ℹ️  Using Basic Auth (Username/Password)")
        config = BitbucketBasicAuthConfig(
            username=USERNAME,
            password=PASSWORD,
            base_url=BASE_URL
        )
        client = BitbucketClient.build_with_config(config)
    else:
        print("⚠️  Please set BITBUCKET_CLIENT_ID/SECRET for OAuth OR BITBUCKET_USERNAME/PASSWORD.")
        return

    # Ensure we have a valid client before proceeding
    if client is None:
        print("❌ Failed to initialize client. Cannot proceed with API calls.")
        return

    # --- Standard API Usage ---
    data_source = BitbucketDataSource(client)
    print("Client initialized successfully.")

    # 2. Get Current User
    print_section("Current User")
    user_resp = await data_source.get_user()
    print_result("Get User", user_resp)

    # 3. List Workspaces
    print_section("Workspaces")
    workspaces_resp = await data_source.get_workspaces()
    print_result("List Workspaces", workspaces_resp)

    # 4. List Repositories in Workspace
    print_section("Repositories")
    repositories = []
    if workspaces_resp.success and workspaces_resp.data:
        workspaces = workspaces_resp.data.get("values", []) if isinstance(workspaces_resp.data, dict) else workspaces_resp.data
        for workspace in workspaces:
            if isinstance(workspace, dict):
                workspace_id = workspace.get("slug") or workspace.get("uuid", "").strip("{}")
            else:
                workspace_id = workspace
            print("workspace:", workspace)
            repos_resp = await data_source.get_repositories_workspace(
                workspace=workspace_id,
                sort="-updated_on" # Get recently updated repos
            )
            print_result("List Repositories", repos_resp)
            print("repos_resp:", repos_resp.data)

            # Extract repositories from response
            if repos_resp.success and repos_resp.data:
                repos_data = repos_resp.data.get("values", []) if isinstance(repos_resp.data, dict) else repos_resp.data
                for repo in repos_data:
                    if isinstance(repo, dict):
                        repo_workspace = repo.get("workspace", {})
                        if isinstance(repo_workspace, dict):
                            repo_workspace_slug = repo_workspace.get("slug")
                        else:
                            repo_workspace_slug = workspace_id
                        repo_slug = repo.get("slug")
                        if repo_workspace_slug and repo_slug:
                            repositories.append({
                                "workspace": repo_workspace_slug,
                                "repo_slug": repo_slug,
                                "full_name": repo.get("full_name", f"{repo_workspace_slug}/{repo_slug}")
                            })

    # Process each repository found
    for repo_info in repositories:
        workspace_slug = repo_info["workspace"]
        repo_slug = repo_info["repo_slug"]
        full_name = repo_info["full_name"]

        # 5. Get Specific Repository Details
        print_section(f"Details for {full_name}")
        repo_detail = await data_source.get_repositories_workspace_repo_slug(
            workspace=workspace_slug,
            repo_slug=repo_slug
        )
        print_result("Get Single Repository", repo_detail)

        # 6. List Commits
        print_section(f"Commits for {full_name}")
        commits_resp = await data_source.get_repositories_workspace_repo_slug_commits(
            workspace=workspace_slug,
            repo_slug=repo_slug
        )
        print_result("List Commits", commits_resp)

        # 7. List Pull Requests
        print_section(f"Pull Requests for {full_name}")
        prs_resp = await data_source.get_repositories_workspace_repo_slug_pullrequests(
            workspace=workspace_slug,
            repo_slug=repo_slug,
            state="OPEN"
        )
        print_result("List Open Pull Requests", prs_resp)

        # 8. List Issues (Deprecated - Bitbucket Cloud no longer supports this endpoint)
        print_section(f"Issues for {full_name}")
        print("⚠️  Note: Bitbucket Cloud API v2.0 no longer supports the issues endpoint.")
        print("   Issues are now managed through Jira integration.")
        # Skipping the API call as it will always fail

        # 9. List Branches (Refs)
        print_section(f"Branches for {full_name}")
        branches_resp = await data_source.get_repositories_workspace_repo_slug_refs_branches(
            workspace=workspace_slug,
            repo_slug=repo_slug
        )
        print_result("List Branches", branches_resp)


if __name__ == "__main__":
    asyncio.run(main())