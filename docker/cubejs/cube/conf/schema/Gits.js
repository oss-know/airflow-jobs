cube(`Gits`, {
  sql: `SELECT * FROM default.gits`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [authorName, committerName, filesFileName, authoredDate, committedDate]
    }
  },
  
  dimensions: {
    searchKeyOwner: {
      sql: `search_key__owner`,
      type: `string`,
      title: `Search Key  Owner`
    },
    
    searchKeyRepo: {
      sql: `search_key__repo`,
      type: `string`,
      title: `Search Key  Repo`
    },
    
    searchKeyOrigin: {
      sql: `search_key__origin`,
      type: `string`,
      title: `Search Key  Origin`
    },
    
    message: {
      sql: `message`,
      type: `string`
    },
    
    hexsha: {
      sql: `hexsha`,
      type: `string`
    },
    
    parents: {
      sql: `parents`,
      type: `string`
    },
    
    authorName: {
      sql: `author_name`,
      type: `string`
    },
    
    authorEmail: {
      sql: `author_email`,
      type: `string`
    },
    
    committerName: {
      sql: `committer_name`,
      type: `string`
    },
    
    committerEmail: {
      sql: `committer_email`,
      type: `string`
    },
    
    filesFileName: {
      sql: `${CUBE}."files.file_name"`,
      type: `string`,
      title: `Files.file Name`
    },
    
    type: {
      sql: `type`,
      type: `string`
    },
    
    authoredDate: {
      sql: `authored_date`,
      type: `time`
    },
    
    committedDate: {
      sql: `committed_date`,
      type: `time`
    }
  },
  
  dataSource: `default`
});
