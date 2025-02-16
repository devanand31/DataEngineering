/****** Object:  Table [CURATED].[CUSTOMER]    Script Date: 16-02-2025 22:30:21 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [CURATED].[CUSTOMER](
	[CUSTOMER_ID] [nvarchar](max) NULL,
	[NAME] [nvarchar](max) NULL,
	[EMAIL] [nvarchar](max) NULL,
	[CITY_ID] [nvarchar](max) NULL,
	[GENDER] [nvarchar](max) NULL,
	[SIGNUP_DATE] [date] NULL,
	[AGE_GROUP] [nvarchar](max) NULL,
	[EFFECTIVE_START_DATE] [date] NULL,
	[EFFECTIVE_END_DATE] [date] NULL,
	[ACTIVE_IND] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

