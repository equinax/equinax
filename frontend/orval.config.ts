import { defineConfig } from 'orval'

export default defineConfig({
  quantBacktest: {
    input: {
      target: 'http://localhost:8000/api/openapi.json',
    },
    output: {
      mode: 'tags-split',
      target: 'src/api/generated',
      schemas: 'src/api/generated/schemas',
      client: 'react-query',
      override: {
        mutator: {
          path: 'src/api/mutator.ts',
          name: 'customInstance',
        },
        query: {
          useQuery: true,
          useMutation: true,
          useInfinite: true,
          useInfiniteQueryParam: 'page',
        },
      },
    },
    hooks: {
      afterAllFilesWrite: 'prettier --write',
    },
  },
})
