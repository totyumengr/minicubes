create table minicube (
	dim_the_date INT not null,
    dim_tradeId INT not null,
    dim_productLineId INT not null,
    dim_postId INT not null,
    csm DECIMAL(11,8) not null,
    cash DECIMAL(10,8) not null,
    click BIGINT not null,
    shw BIGINT not null,
    _merge_flag_ INT not null,
    primary key (dim_the_date, dim_tradeId, dim_productLineId, dim_postId)
);
