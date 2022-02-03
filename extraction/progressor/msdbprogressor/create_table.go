package msdbprogressor

import (
	"github.com/ws6/msi"
)

//only on [dbo]
var create_sql = `
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[system_config](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[namespace] [varchar](200) NOT NULL,
	[k] [varchar](200) NOT NULL,
	[string] [varchar](400) NULL,
	[time] [datetime] NULL,
	[CreatedAt] [datetime] NULL,
	[UpdatedAt] [datetime] NULL,
	[number] [bigint] NULL,
 CONSTRAINT [PK_system_config] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY],
 CONSTRAINT [IX_system_config] UNIQUE NONCLUSTERED 
(
	[namespace] ASC,
	[k] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[system_config] ADD  CONSTRAINT [DF_system_config_CreatedAt]  DEFAULT (getdate()) FOR [CreatedAt]
GO

ALTER TABLE [dbo].[system_config] ADD  CONSTRAINT [DF_system_config_UpdatedAt]  DEFAULT (getdate()) FOR [UpdatedAt]
GO

`

//CreateIfNotExistTable leave user to do it
func CreateIfNotExistTable(m *msi.Msi) error {
	return nil
}
