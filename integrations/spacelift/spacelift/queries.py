
POLICIES= """
        query {
          policies {
            id
            name
            type
          }
        }
        """

USERS = """
        query {
          users {
            id
            username
            email
          }
        }
        """

DEPLOYMENTS = """
        query ($status: [RunState!], $after: String) {
          runs(states: $status, first: 100, after: $after) {
            edges {
              node {
                id
                state
                createdAt
              }
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
        """

STACKS = """
        query {
          stacks {
            id
            name
            state
            branch
          }
        }
        """

SPACES = """
        query {
          spaces {
            id
            name
            description
          }
        }
        """





