/***************************************** Head Section *******************************************/
/**  主题 = CDM                                                                                   */
/**  表名 = dim_cstm_dmns_h                                                                       */
/**  作业描述 = 客户维度历史表                                                                    */
/**  开发人员 = 何东美                                                                            */
/**  SQL脚本必须 utf-8 格式                                                                       */
/**  预留变量：${TX_DATE_I}                     # 多天批量多次运行使用                            */
/**  预留变量：${START_TX_DATE} ${END_TX_DATE}  # 多天批量一次运行中使用                          */
/**  预留变量：${TX_DATE_BATCH}                                                                   */
/**  预留变量：${ETL_JOB}                                                                         */
/**  预留变量：${$MAXDATE}                                                                        */
/**  预留变量：${$MINDATE}                                                                        */
/**  预留变量：                                                                                   */
/**  预留变量：                                                                                   */
/*************************************** Head Section End *****************************************/
/*-FOR=N*/

DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h CASCADE;
/*创建临时表*/
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h ON COMMIT PRESERVE ROWS
    AS
SELECT cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
FROM ${CDM_SCHEMA}.dim_cstm_dmns_h
 WHERE 1<>1;

 
/*机构类型代码*/ 
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_orgn_typ CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_orgn_typ ON COMMIT PRESERVE ROWS AS ( 
SELECT  
a1.custid 
,COALESCE(a2.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.institutiontype),''))  orgn_typ_cd /*机构类型代码*/
,a2.trgt_enmr_nm orgn_typ	/*机构类型*/
,'0' custtype
FROM  
${DL_ODS_SCHEMA}.s054_ecif_vworgcust_f a1
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a2
ON 's054+s054_ecif_vworgcust_f+institutiontype+dim_cstm_dmns_h+orgn_typ_cd+'||TRIM(a1.institutiontype) = a2.stnd_cd_mppn_srrg_prmr_key /*机构类型代码*/ 
UNION ALL
SELECT
a1.custid 
,COALESCE(a2.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.institutiontype),''))  orgn_typ_cd /*机构类型代码*/
,a2.trgt_enmr_nm orgn_typ	/*机构类型*/
,'2' custtype
FROM  
${DL_ODS_SCHEMA}.s054_ecif_vwproductcust_f a1
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a2
ON 's054+s054_ecif_vwproductcust_f+institutiontype+dim_cstm_dmns_h+orgn_typ_cd+'||TRIM(a1.institutiontype) = a2.stnd_cd_mppn_srrg_prmr_key /*机构类型代码*/ 
)ORDER BY custid,custtype
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_orgn_typ.custid,tmp_dim_cstm_dmns_h_orgn_typ.custtype) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_orgn_typ');  

/*客户主表*/ 
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_cstm_mian_dt CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_cstm_mian_dt ON COMMIT PRESERVE ROWS AS (   
SELECT 
 a1.custid custid /*客户id*/
,CAST(a1.pid AS VARCHAR)  pcustid /*归并后客户id  20220526 孔晔反馈后修改 从s054_ecif_vwcust_f 出，不再从s054_ecif_tcustmerge_mf出*/
,COALESCE(a5.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.custtype),''))  cstm_typ_cd /*客户类型代码*/
,a5.trgt_enmr_nm cstm_typ /*客户类型*/
,TRIM(a1.custname) custname /*客户名称*/
,TRIM(a1.custsimplename) custsimplename /*客户简称*/ 
,COALESCE(a6.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.custtype),'')||'+'||COALESCE(TRIM(a1.certtype),''))     cstm_crtf_typ_cd /*客户证件类型代码*/
,a6.trgt_enmr_nm cstm_crtf_typ/*客户证件类型*/
,TRIM(a1.certcode) certcode /*证件号码*/ 
,DATE(a1.idnovaliddate) idnovaliddate  /*客户证件有效期*/
,TRIM(a1.country) country /*地址_国家*/
,TRIM(a1.province)  province/*地址_省*/
,TRIM(a1.city)  city/*地址_市*/
,TRIM(a1.address) address /*地址_详细*/
,TRIM(a1.postcode) postcode/*邮编*/
,COALESCE(a2.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.validated),''))  cstm_effc_flg_cd/*客户有效标志代码*/
,a2.trgt_enmr_nm  cstm_effc_flg /*客户有效标志*/
,COALESCE(a3.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.risklevel),''))  cstm_rsk_clss_cd/*客户风险等级代码*/
,a3.trgt_enmr_nm cstm_rsk_clss /*客户风险等级*/
,COALESCE(a4.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.custstatus),''))  cstm_stt_cd /*客户状态代码*/
,a4.trgt_enmr_nm cstm_stt /*客户状态*/
,CASE WHEN a6.trgt_enmr_cd = '1+0' AND LENGTH(TRIM(a1.certcode)) = 18 
       THEN  TRIM(a1.certcode)
       WHEN a6.trgt_enmr_cd = '1+0' AND LENGTH(TRIM(a1.certcode))   = 15 
       THEN idtransfer(TRIM(a1.certcode), 1) 
       ELSE NULL
  END cstm_18_id_crd_nmbr
,COALESCE(CASE WHEN a6.trgt_enmr_cd = '1+0' AND LENGTH(TRIM(a1.certcode)) = 18 
       THEN  TRIM(a1.certcode)
       WHEN a6.trgt_enmr_cd = '1+0' AND LENGTH(TRIM(a1.certcode))   = 15 
       THEN idtransfer(TRIM(a1.certcode), 1) 
       ELSE NULL
  END,TRIM(a1.certcode)) cstm_id_crd_nmbr_x
,TRIM(a1.county)  cstm_addr_dstr	    /*客户地址_区*/
,COALESCE(a12.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.custtype),'')||'+'||COALESCE(TRIM(a1.auxcerttype),''))     sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,a12.trgt_enmr_nm sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,TRIM(a1.auxcertcode) sppr_id_crtf_fl_nmbr	/*辅助身份证明文件号码*/
,DATE(TRIM(a1.auxvaliddate)) sppr_id_crtf_fl_avlb_prd	/*辅助身份证明文件有效期*/
,a1.custtype
FROM  ${DL_ODS_SCHEMA}.s054_ecif_vwcust_f a1
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a2
ON 's054+s054_ecif_vwcust_f+validated+dim_cstm_dmns_h+cstm_effc_flg_cd+'||TRIM(a1.validated) = a2.stnd_cd_mppn_srrg_prmr_key /*客户有效标志*/
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a3
ON 's054+s054_ecif_vwcust_f+risklevel+dim_cstm_dmns_h+cstm_rsk_clss_cd+'||TRIM(a1.risklevel) = a3.stnd_cd_mppn_srrg_prmr_key /*客户风险等级*/
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a4
ON 's054+s054_ecif_vwcust_f+custstatus+dim_cstm_dmns_h+cstm_stt_cd+'||TRIM(a1.custstatus) = a4.stnd_cd_mppn_srrg_prmr_key /*客户状态*/
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a5
ON 's054+s054_ecif_vwcust_f+custtype+dim_cstm_dmns_h+cstm_typ_cd+'||TRIM(a1.custtype) = a5.stnd_cd_mppn_srrg_prmr_key /*客户类型*/
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a6
ON 's054+s054_ecif_vwcust_f+custtype+certtype+dim_cstm_dmns_h+cstm_crtf_typ_cd+'||COALESCE(TRIM(a1.custtype),'')||'+'||COALESCE(TRIM(a1.certtype),'') = a6.stnd_cd_mppn_srrg_prmr_key /*客户证件类型*/  
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a12
ON 's054+s054_ecif_vwcust_f+custtype+auxcerttype+dim_cstm_dmns_h+sppr_id_crtf_fl_typ_cd+'||COALESCE(TRIM(a1.custtype),'')||'+'||COALESCE(TRIM(a1.auxcerttype),'') = a12.stnd_cd_mppn_srrg_prmr_key /*辅助身份证明文件类型代码*/ 
)
ORDER BY custid
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_cstm_mian_dt.custid) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_cstm_mian_dt');

/*个人客户信息*/ 
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_vwpersoncust CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_vwpersoncust ON COMMIT PRESERVE ROWS AS (   
SELECT 
 a8.custid custid /*客户id*/
,a8.birthday  birthday/*出生日期*/
,TRIM(a8.nationality) nationality /*国籍*/
,TRIM(a8.educationlevel) educationlevel /*学历*/
,TRIM(a8.mobilephone) mobilephone /*移动电话*/
,COALESCE(TRIM(a8.phone)) phone /*固话*/
,TRIM(a8.officephone) officephone /*办公电话*/
,COALESCE(TRIM(a8.email))   email/*电子邮箱*/
,COALESCE(a9.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a8.sex),''))  gndr_cd/*性别代码*/
,a9.trgt_enmr_nm gndr/*性别*/
,TRIM(a8.workplace) workaddress /*单位地址*/
,COALESCE(a10.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a8.educationlevel),''))  dplm_cd /*学历代码*/
,a10.trgt_enmr_nm dplm /*学历*/
,a8.penates  fmly_asst /*家庭资产*/
,COALESCE(a11.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a8.marriagestatus),''))  mrtl_stts_cd /*婚姻状况代码*/
,a11.trgt_enmr_nm mrtl_stts /*婚姻状况*/
,a8.enfamliyname  cstm_eng_fmly_nm    /*客户英文姓氏*/
,a8.enfirstname   cstm_eng_nm	        /*客户英文名称*/
,COALESCE(TRIM(a8.fax))  fax /*传真*/
,a8.annualincome cstm_yr_incm	/*客户年收入*/
,a13.admn_rgn_srrg_key  admn_rgn_srrg_key /*客户国籍*/
,a8.familynum fmly_nmbr_of_ppl	/*家庭人数*/
,COALESCE(a14.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a8.vocation),''))  crr_cd /*职业代码*/
,a14.trgt_enmr_nm crr	/*职业*/
FROM  ${DL_ODS_SCHEMA}.s054_ecif_vwpersoncust_f a8 /*个人客户*/ 
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a9
ON 's054+s054_ecif_vwpersoncust_f+sex+dim_cstm_dmns_h+gndr_cd+'||TRIM(a8.sex) = a9.stnd_cd_mppn_srrg_prmr_key /*个人客户性别*/
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a10
ON 's054+s054_ecif_vwpersoncust_f+educationlevel+dim_cstm_dmns_h+dplm_cd+'||TRIM(a8.educationlevel) = a10.stnd_cd_mppn_srrg_prmr_key /*个人客户学历*/
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a11
ON 's054+s054_ecif_vwpersoncust_f+marriagestatus+dim_cstm_dmns_h+mrtl_stts_cd+'||TRIM(a8.marriagestatus) = a11.stnd_cd_mppn_srrg_prmr_key /*个人婚姻状况*/  
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a14
ON 's054+s054_ecif_vwpersoncust_f+vocation+dim_cstm_dmns_h+crr_cd+'||TRIM(a8.vocation) = a14.stnd_cd_mppn_srrg_prmr_key /*职业代码*/ 
LEFT JOIN  (
SELECT DISTINCT admn_rgn_attr_key
               ,admn_rgn_srrg_key
  FROM ${CDM_SCHEMA}.dim_extn_cntr_admn_rgn_dmns
 WHERE admn_rgn_attr_id IN ('cntrnm_en2', 'cntrnm_en3', 'cntrnm_nmbr3')) a13
ON TRIM(a8.nationality) = a13.admn_rgn_attr_key /*客户国籍代码*/  
)
ORDER BY custid
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_vwpersoncust.custid) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_vwpersoncust'); 



/*客户主表*/ 
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_vwcust_f CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_vwcust_f ON COMMIT PRESERVE ROWS AS (   
SELECT 
a1.custid
,a1.pcustid
,a1.cstm_typ_cd
,a1.cstm_typ
,a1.custname
,a1.custsimplename
,a1.cstm_crtf_typ_cd
,a1.cstm_crtf_typ
,a1.certcode
,a1.idnovaliddate
,a1.country
,a1.province
,a1.city
,a1.address
,a1.postcode
,a1.cstm_effc_flg_cd
,a1.cstm_effc_flg
,a1.cstm_rsk_clss_cd
,a1.cstm_rsk_clss
,a1.cstm_stt_cd
,a1.cstm_stt
,a1.cstm_18_id_crd_nmbr
,a1.cstm_id_crd_nmbr_x
,a1.cstm_addr_dstr
,a1.sppr_id_crtf_fl_typ_cd
,a1.sppr_id_crtf_fl_typ
,a1.sppr_id_crtf_fl_nmbr
,a1.sppr_id_crtf_fl_avlb_prd
,a2.birthday
,a2.nationality
,a2.educationlevel
,a2.mobilephone
,a2.phone
,a2.officephone
,a2.email
,COALESCE(a2.gndr_cd,'UNKN_') gndr_cd /*性别代码，保持和优化前代码一致，不然会出现多条拉链*/
,a2.gndr
,a2.workaddress
,COALESCE(a2.dplm_cd,'UNKN_') dplm_cd /*学历代码，保持和优化前代码一致，不然会出现多条拉链*/
,a2.dplm
,a2.fmly_asst
,COALESCE(a2.mrtl_stts_cd,'UNKN_') mrtl_stts_cd /*婚姻状况代码，保持和优化前代码一致，不然会出现多条拉链*/
,a2.mrtl_stts
,a2.cstm_eng_fmly_nm
,a2.cstm_eng_nm
,a2.fax
,a2.cstm_yr_incm
,a2.admn_rgn_srrg_key
,a2.fmly_nmbr_of_ppl
,COALESCE(a2.crr_cd,'UNKN_') crr_cd /*职业代码，保持和优化前代码一致，不然会出现多条拉链*/
,a2.crr
,a1.custtype
 FROM tmp_dim_cstm_dmns_h_cstm_mian_dt a1
 LEFT JOIN tmp_dim_cstm_dmns_h_vwpersoncust a2
 ON a1.custid=a2.custid AND
    a1.custtype='1'
)
ORDER BY custid,custtype
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_vwcust_f.custid,tmp_dim_cstm_dmns_h_vwcust_f.custtype) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_vwcust_f');  


/*vworgcust*/
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_vworgcust_f CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_vworgcust_f ON COMMIT PRESERVE ROWS AS (   
SELECT 
a16.custid
,a16.regcapital orgn_rgst_cptl	/*机构注册资本*/
,a16.stuffnum orgn_empl_nmbr_of_ppl	/*机构员工人数*/
,TRIM(a16.instreprname) orgn_lgl_entt_nm	/*机构法人姓名*/
,TRIM(a16.instrepridcode) orgn_lgl_entt_crtf_nmbr	/*机构法人证件号码*/
,DATE(a16.instreprcertvaliddate) orgn_lgl_entt_avlb_prd	/*机构法人证件有效期*/
,TRIM(a16.orgno) orgn_orgn_id	/*组织机构编号*/
,COALESCE(a18.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a16.vocation),''))  orgn_inds_cd /*机构行业代码*/
,a18.trgt_enmr_nm orgn_inds	/*机构行业*/
,TRIM(a16.managerange)  orgn_mngm_rng	/*机构经营范围*/  /*于 20240628 修改 由instreprmanagerange 改为managerange（上游系统是20240621上线）*/
,TRIM(a16.phone) phone
,TRIM(a16.email) email
,TRIM(a16.fax) fax
 FROM ${DL_ODS_SCHEMA}.s054_ecif_vworgcust_f a16
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a18
ON 's054+s054_ecif_vworgcust_f+vocation+dim_cstm_dmns_h+orgn_inds_cd+'||TRIM(a16.vocation) = a18.stnd_cd_mppn_srrg_prmr_key /*机构行业代码*/ 
)ORDER BY custid
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_vworgcust_f.custid) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_vworgcust_f'); 


/*机构成立时间*/
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_torganization_mf CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_torganization_mf ON COMMIT PRESERVE ROWS AS (   
SELECT custid
,DATE(estiblishdate) orgn_estb_tm	/*机构成立时间*/
 FROM ${DL_ODS_SCHEMA}.s054_ecif_torganization_mf  
WHERE pt= '${TX_DATE_BATCH}'
)
ORDER BY custid
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_torganization_mf.custid) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_torganization_mf'); 


/*ECIF客户信息*/ 
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_ecif_cust_info CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_ecif_cust_info ON COMMIT PRESERVE ROWS AS (
SELECT 
 CAST(a1.custid AS VARCHAR) custid
,a1.pcustid
,a1.cstm_typ_cd
,a1.cstm_typ
,a1.custname
,a1.custsimplename
,a1.cstm_crtf_typ_cd
,a1.cstm_crtf_typ
,a1.certcode
,a1.idnovaliddate
,a1.country
,a1.province
,a1.city
,a1.address
,a1.postcode
,a1.birthday
,a1.nationality
,a1.educationlevel
,a1.mobilephone
,COALESCE(a1.phone,a16.phone) phone /*固话*/
,a1.officephone
,COALESCE(a1.email,a16.email)   email/*电子邮箱*/
,a1.gndr_cd
,a1.gndr
,a1.workaddress
,a1.cstm_effc_flg_cd
,a1.cstm_effc_flg
,a1.cstm_rsk_clss_cd
,a1.cstm_rsk_clss
,a1.cstm_stt_cd
,a1.cstm_stt
,a1.dplm_cd
,a1.dplm
,a1.fmly_asst
,a1.mrtl_stts_cd
,a1.mrtl_stts
,a1.cstm_18_id_crd_nmbr
,a1.cstm_id_crd_nmbr_x
,a1.cstm_eng_fmly_nm
,a1.cstm_eng_nm
,a1.cstm_addr_dstr
,a1.sppr_id_crtf_fl_typ_cd
,a1.sppr_id_crtf_fl_typ
,a1.sppr_id_crtf_fl_nmbr
,a1.sppr_id_crtf_fl_avlb_prd
,COALESCE(a1.fax,a16.fax)  fax /*传真*/
,a1.cstm_yr_incm
,a1.admn_rgn_srrg_key
,a1.fmly_nmbr_of_ppl
,a1.crr_cd
,a1.crr
,a15.orgn_typ_cd	/*机构类型代码*/
,a15.orgn_typ	/*机构类型*/
,a16.orgn_rgst_cptl
,a16.orgn_empl_nmbr_of_ppl
,a16.orgn_lgl_entt_nm
,a16.orgn_lgl_entt_crtf_nmbr
,a16.orgn_lgl_entt_avlb_prd
,a16.orgn_orgn_id
,COALESCE(a16.orgn_inds_cd,'UNKN_') orgn_inds_cd
,a16.orgn_inds
,a16.orgn_mngm_rng
,a17.orgn_estb_tm	/*机构成立时间*/
FROM  tmp_dim_cstm_dmns_h_vwcust_f a1
LEFT JOIN tmp_dim_cstm_dmns_h_orgn_typ a15  /*机构类型*/
ON a1.custid =a15.custid and
   a1.custtype=a15.custtype
LEFT JOIN tmp_dim_cstm_dmns_h_vworgcust_f a16 /*机构客户*/ 
ON a1.custid =a16.custid  AND
   a1.custtype='0'
LEFT JOIN tmp_dim_cstm_dmns_h_torganization_mf a17  
ON a1.custid =a17.custid 
)
ORDER BY custid
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_ecif_cust_info.custid) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_ecif_cust_info');


/*风险测评是否有效*/
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_if_rsk_asss_effc CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_if_rsk_asss_effc ON COMMIT PRESERVE ROWS AS (
WITH  tmp_dim_cstm_dmns_h_if_rsk_asss_effc_001 AS (
SELECT
a1.vc_custno
,a1.vc_replydate
,a1.c_custrisktype
,a1.vc_questionnaireno
,ROW_NUMBER() OVER(PARTITION BY a1.vc_custno ORDER BY a1.vc_questionnaireno DESC) rank
FROM  ${DL_ODS_SCHEMA}.s010_hssale30_thcustriskinfo_mf a1
WHERE a1.pt  = '${TX_DATE_BATCH}' AND
 a1.vc_custno NOT IN (/*剔除在当前表里的*/
 SELECT vc_custno FROM ${DL_ODS_SCHEMA}.s010_hssale30_tcustriskinfo_mf  
 WHERE pt  = '${TX_DATE_BATCH}' AND  vc_custno IS NOT NULL)
 AND a1.vc_custno IS NOT NULL
),tmp_dim_cstm_dmns_h_if_rsk_asss_effc_002 AS (
SELECT  
DISTINCT COALESCE(a2.c_custtype, '') || '+' || COALESCE(a2.c_identitytype, '') cert_type
               ,TRIM(a2.c_custtype) cstm_type
               ,CASE WHEN COALESCE(a2.c_custtype, '') || '+' || COALESCE(a2.c_identitytype, '') = '1+0' AND LENGTH(a2.vc_identityno) = 15 THEN
                     idtransfer(UPPER(TRIM(a2.vc_identityno)), 1)
                   ELSE UPPER(TRIM(a2.vc_identityno))
                   END cert_no
               ,TRIM(a2.vc_customname) cust_name
               ,DATE(a1.vc_replydate) vc_replydate
               ,TRIM(a1.c_custrisktype) c_custrisktype
FROM tmp_dim_cstm_dmns_h_if_rsk_asss_effc_001  a1
LEFT JOIN ${DL_ODS_SCHEMA}.s010_hssale30_tcustinfo_mf a2
ON a1.vc_custno=a2.vc_custno AND
      a2.pt  = '${TX_DATE_BATCH}'  
WHERE a1.rank=1     
),

 tmp_dim_cstm_dmns_h_ax AS (
SELECT  COALESCE(a1.c_custtype, '') || '+' || COALESCE(a1.c_identitytype, '') cert_type
               ,TRIM(a1.c_custtype) cstm_type
               ,CASE WHEN COALESCE(a1.c_custtype, '') || '+' || COALESCE(a1.c_identitytype, '') = '1+0' AND LENGTH(a1.vc_identityno) = 15 THEN
                     idtransfer(UPPER(TRIM(a1.vc_identityno)), 1)
                   ELSE UPPER(TRIM(a1.vc_identityno))
                   END cert_no
               ,TRIM(a1.vc_customname) cust_name
               ,DATE(a1.vc_replydate) vc_replydate
               ,TRIM(a1.c_custrisktype) c_custrisktype
  FROM ${DL_ODS_SCHEMA}.s010_hssale30_tcustriskinfo_mf a1
 WHERE a1.pt  = '${TX_DATE_BATCH}'
UNION 
/*并集历史风险等级数据*/
SELECT 
cert_type
,cstm_type
,cert_no
,cust_name
,vc_replydate
,c_custrisktype
FROM tmp_dim_cstm_dmns_h_if_rsk_asss_effc_002 
)
SELECT  
a1.cert_type
,a1.cstm_type
,a1.cert_no
,a1.cust_name
,CASE  WHEN TO_DATE('${TX_DATE_I}', 'YYYYMMDD') - a1.vc_replydate <= 365
          THEN  'Y'
           ELSE 'N'
END if_rsk_asss_effc /*小于1年算有效*/
,a1.c_custrisktype
,COALESCE(a2.trgt_enmr_cd,'UNKN_'||COALESCE(TRIM(a1.c_custrisktype),'')) rsk_asss_clss_cd /*风险测评等级代码*/
,a2.trgt_enmr_nm rsk_asss_clss	/*风险测评等级*/
FROM (
SELECT a1.cert_type
,a1.cstm_type
,a1.cert_no
,a1.cust_name
,a1.vc_replydate
,a1.c_custrisktype
,ROW_NUMBER() OVER(PARTITION BY a1.cert_type,a1.cstm_type,a1.cert_no,a1.cust_name ORDER BY  a1.vc_replydate DESC) rank
/*若有多次风险测评，取最近日期的数*/
 FROM  tmp_dim_cstm_dmns_h_ax a1
) a1
LEFT JOIN ${CDM_SCHEMA}.para_stnd_cd_mppn a2
ON 's010+s010_hssale30_tcustriskinfo_mf+c_custrisktype+dim_cstm_dmns_h+rsk_asss_clss_cd+'||a1.c_custrisktype = a2.stnd_cd_mppn_srrg_prmr_key /*风险测评等级代码*/
WHERE a1.rank=1
)ORDER BY cert_type,cstm_type,cert_no,cust_name
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_if_rsk_asss_effc.cert_no) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_if_rsk_asss_effc');





/*是否黑名单客户  是否添加企业微信  线下活动参与情况*/
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_t_stat_kh_khtz CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_t_stat_kh_khtz ON COMMIT PRESERVE ROWS AS (
SELECT
DISTINCT a1.khh cstm_srrg_key
               ,CASE
                    WHEN a1.hdhmdkh IS NULL THEN
                     'N'
                    ELSE
                     'Y'
                END if_blck_cstm /*是否黑名单客户*/
,COALESCE(a2.trgt_enmr_cd,'UNKN_'||COALESCE(TO_CHAR(a1.qywxzt),'N')) entr_wx_add_stt_cd /*企业微信添加状态代码*/
,a2.trgt_enmr_nm entr_wx_add_stt	/*企业微信添加状态*/
,a1.xxhdkh offl_actv_stts        /*线下活动参与情况*/                    
FROM ${DL_ODS_SCHEMA}.s060_dsc_data_t_stat_kh_khtz_mf a1
LEFT JOIN  ${CDM_SCHEMA}.para_stnd_cd_mppn a2
ON 's060+s060_dsc_data_t_stat_kh_khtz_mf+qywxzt+dim_cstm_dmns_h+entr_wx_add_stt_cd+'||COALESCE(TO_CHAR(a1.qywxzt),'N') = a2.stnd_cd_mppn_srrg_prmr_key /*企业微信添加状态代码*/
 WHERE a1.pt  = '${TX_DATE_BATCH}'
)ORDER BY khh
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_t_stat_kh_khtz.cstm_srrg_key) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_t_stat_kh_khtz'); 
 

/*是否接受营销广告*/
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_if_accp_mkt_advr CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_if_accp_mkt_advr ON COMMIT PRESERVE ROWS AS (
SELECT DISTINCT a1.khh cstm_srrg_key
               ,'Y' if_accp_mkt_advr
  FROM ${DL_ODS_SCHEMA}.s060_jjcrm_tjj_khyxxx_mf a1
 WHERE a1.yxbz = '1' AND 
       a1.pt  = '${TX_DATE_BATCH}'
)ORDER BY khh
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_if_accp_mkt_advr.cstm_srrg_key) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_if_accp_mkt_advr'); 

/*是否完成证件照片上传*/
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_if_upld_id_pht CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_if_upld_id_pht ON COMMIT PRESERVE ROWS AS (
select distinct '1+'||a1.cert_type  cert_type
,'1'  cstm_type
,CASE WHEN '1+'||a1.cert_type = '1+0' AND LENGTH(TRIM(a1.cert_no)) = 15 THEN
            idtransfer(TRIM(a1.cert_no), 1)
           ELSE TRIM(a1.cert_no)
       END cert_no
,TRIM(a1.cust_name) cust_name
,'Y' if_upld_id_pht
FROM ${DL_ODS_SCHEMA}.s033_fcupadm_picture_recorg_info_mf a1
WHERE a1.pt  = '${TX_DATE_BATCH}' 
)
ORDER BY cert_type,cstm_type,cert_no,cust_name
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_if_upld_id_pht.cert_no) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_if_upld_id_pht');  

/*出结果优化1*/
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_cstm_results_x01  CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_cstm_results_x01 ON COMMIT PRESERVE ROWS AS (
SELECT     a1.custid cstm_srrg_key /*客户代理键（客户号）*/
          ,a1.custid /*客户号*/
          ,a1.pcustid aftr_mrg_cstm_id /*归并后客户号*/
          ,a1.cstm_typ_cd cstm_typ_cd /*客户类型代码*/
          ,a1.cstm_typ /*客户类型*/
          ,a1.custname cstm_fll_nm /*客户全称*/
          ,a1.custsimplename cstm_abbr /*客户简称*/
          ,a1.cstm_crtf_typ_cd cstm_crtf_typ_cd /*客户证件类型代码*/
          ,a1.cstm_crtf_typ /*客户证件类型*/
          ,a1.certcode cstm_crtf_nmbr /*客户证件号码*/
          ,a1.idnovaliddate cstm_crtf_avlb_prd /*客户证件有效期*/
          ,a1.country cstm_addr_cntr /*客户地址_国家*/
          ,a1.province cstm_addr_prov /*客户地址_省*/
          ,a1.city cstm_addr_cty /*客户地址_市*/
          ,a1.address cstm_addr_dtld /*客户地址_详细*/
          ,a1.postcode cstm_zip /*客户邮编*/
          ,a1.cstm_rsk_clss_cd cstm_rsk_clss_cd /*客户风险等级代码*/
          ,a1.cstm_rsk_clss cstm_rsk_clss /*客户风险等级*/
          ,CASE WHEN a1.cstm_typ_cd ='1' THEN a1.gndr_cd ELSE NULL END gndr_cd /*性别代码*/
          ,CASE WHEN a1.cstm_typ_cd ='1' THEN a1.gndr ELSE NULL END gndr /*性别*/
          ,a1.birthday brth /*出生日期*/
          ,a1.mobilephone phn /*移动电话*/
          ,a1.phone tlph /*固话*/
          ,a1.officephone offc_phn /*办公电话*/
          ,a1.email eml /*电子邮箱*/
          ,a1.workaddress wrk_inst /*工作单位*/
          ,a1.cstm_stt_cd cstm_stt_cd /*客户状态代码*/
          ,a1.cstm_stt /*客户状态*/
          ,a1.cstm_effc_flg_cd cstm_effc_flg_cd /*客户有效标志代码*/
          ,a1.cstm_effc_flg cstm_effc_flg /*客户有效标志*/
          ,CASE WHEN a1.cstm_typ_cd ='1' THEN a1.dplm_cd ELSE NULL END dplm_cd /*学历代码*/
          ,CASE WHEN a1.cstm_typ_cd ='1' THEN a1.dplm ELSE NULL END dplm  /*学历*/
          ,a1.fmly_asst fmly_asst /*家庭资产*/
          ,CASE WHEN a1.cstm_typ_cd ='1' THEN a1.mrtl_stts_cd ELSE NULL END mrtl_stts_cd /*婚姻状况代码*/
          ,CASE WHEN a1.cstm_typ_cd ='1' THEN a1.mrtl_stts ELSE NULL END  mrtl_stts /*婚姻状况*/
          /*,a2.if_rsk_asss_effc*/ /*风险测评是否有效*/
          /*,a2.rsk_asss_clss_cd*/	/*风险测评等级代码*/
          /*,a2.rsk_asss_clss	*/   /*风险测评等级*/
          ,COALESCE(a4.if_accp_mkt_advr,'N') if_accp_mkt_advr /*是否接受营销广告*/
          ,COALESCE(a3.if_blck_cstm,'N')  if_blck_cstm /*是否黑名单客户*/
          /*,COALESCE(a6.if_intr_rltn,'N') if_intr_empl*/ /*是否内部员工*/
          /*,COALESCE(a7.if_intr_rltn,'N') if_intr_empl_fmly *//*是否内部员工家属*/
          /*,COALESCE(a5.if_upld_id_pht,'N') if_upld_id_pht*/ /*是否完成证件照片上传*/
          ,COALESCE(a3.entr_wx_add_stt_cd,'N') entr_wx_add_stt_cd  /*企业微信添加状态代码*/
          ,COALESCE(a3.entr_wx_add_stt,'未添加企业微信') entr_wx_add_stt  /*企业微信添加状态*/
          ,a3.offl_actv_stts /*线下活动参与情况*/
          ,a1.cstm_18_id_crd_nmbr
          ,a1.cstm_eng_fmly_nm
          ,a1.cstm_eng_nm
          ,a1.cstm_addr_dstr
          ,a1.sppr_id_crtf_fl_typ_cd
          ,a1.sppr_id_crtf_fl_typ
          ,a1.sppr_id_crtf_fl_nmbr
          ,a1.sppr_id_crtf_fl_avlb_prd
          ,a1.fax
          ,a1.cstm_yr_incm
          ,a1.fmly_nmbr_of_ppl
          ,CASE WHEN a1.cstm_typ_cd ='1' THEN a1.crr_cd ELSE NULL END  crr_cd
          ,CASE WHEN a1.cstm_typ_cd ='1' THEN a1.crr ELSE NULL END crr
          ,a1.orgn_typ_cd
          ,a1.orgn_typ
          ,a1.orgn_rgst_cptl
          ,a1.orgn_empl_nmbr_of_ppl
          ,a1.orgn_estb_tm
          ,a1.orgn_lgl_entt_nm
          ,a1.orgn_lgl_entt_crtf_nmbr
          ,a1.orgn_lgl_entt_avlb_prd
          ,a1.orgn_orgn_id
          ,CASE WHEN a1.cstm_typ_cd ='0' THEN a1.orgn_inds_cd ELSE NULL END orgn_inds_cd
          ,CASE WHEN a1.cstm_typ_cd ='0' THEN a1.orgn_inds ELSE NULL END  orgn_inds
          ,a1.orgn_mngm_rng
          /*,COALESCE(a6.empl_srrg_key,a7.empl_srrg_key,a8.empl_srrg_key) empl_srrg_key*/ /*员工代理键（员工KMID）*/
          /*,COALESCE(a6.assc_intr_empl_rltn_cd,a7.assc_intr_empl_rltn_cd)	assc_intr_empl_rltn_cd */ /*关联内部员工关系代码*/
          /*,COALESCE(a6.assc_intr_empl_rltn,a7.assc_intr_empl_rltn)	assc_intr_empl_rltn */ /*关联内部员工关系*/
          ,a1.admn_rgn_srrg_key
          ,a1.cstm_id_crd_nmbr_x
      FROM tmp_dim_cstm_dmns_h_ecif_cust_info a1
      LEFT JOIN tmp_dim_cstm_dmns_h_t_stat_kh_khtz a3
        ON a1.custid = a3.cstm_srrg_key
      LEFT JOIN tmp_dim_cstm_dmns_h_if_accp_mkt_advr a4
        ON a1.custid = a4.cstm_srrg_key
)ORDER BY cstm_crtf_typ_cd,cstm_typ_cd,cstm_id_crd_nmbr_x,cstm_fll_nm
SEGMENTED BY hash(tmp_dim_cstm_dmns_h_cstm_results_x01.cstm_crtf_typ_cd
,tmp_dim_cstm_dmns_h_cstm_results_x01.cstm_typ_cd
,tmp_dim_cstm_dmns_h_cstm_results_x01.cstm_id_crd_nmbr_x
,tmp_dim_cstm_dmns_h_cstm_results_x01.cstm_fll_nm
) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_cstm_results_x01');  

/*出结果优化2*/
DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_cstm_results_x02  CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_cstm_results_x02 ON COMMIT PRESERVE ROWS AS (
SELECT a1.cstm_srrg_key /*客户代理键（客户号）*/
          ,a1.custid /*客户号*/
          ,a1.aftr_mrg_cstm_id /*归并后客户号*/
          ,a1.cstm_typ_cd /*客户类型代码*/
          ,a1.cstm_typ /*客户类型*/
          ,a1.cstm_fll_nm /*客户全称*/
          ,a1.cstm_abbr /*客户简称*/
          ,a1.cstm_crtf_typ_cd /*客户证件类型代码*/
          ,a1.cstm_crtf_typ /*客户证件类型*/
          ,a1.cstm_crtf_nmbr /*客户证件号码*/
          ,a1.cstm_crtf_avlb_prd /*客户证件有效期*/
          ,a1.cstm_addr_cntr /*客户地址_国家*/
          ,a1.cstm_addr_prov /*客户地址_省*/
          ,a1.cstm_addr_cty /*客户地址_市*/
          ,a1.cstm_addr_dtld /*客户地址_详细*/
          ,a1.cstm_zip /*客户邮编*/
          ,a1.cstm_rsk_clss_cd /*客户风险等级代码*/
          ,a1.cstm_rsk_clss /*客户风险等级*/
          ,a1.gndr_cd /*性别代码*/
          ,a1.gndr /*性别*/
          ,a1.brth /*出生日期*/
          ,a1.phn /*移动电话*/
          ,a1.tlph /*固话*/
          ,a1.offc_phn /*办公电话*/
          ,a1.eml /*电子邮箱*/
          ,a1.wrk_inst /*工作单位*/
          ,a1.cstm_stt_cd /*客户状态代码*/
          ,a1.cstm_stt /*客户状态*/
          ,a1.cstm_effc_flg_cd /*客户有效标志代码*/
          ,a1.cstm_effc_flg /*客户有效标志*/
          ,a1.dplm_cd /*学历代码*/
          ,a1.dplm  /*学历*/
          ,a1.fmly_asst fmly_asst /*家庭资产*/
          ,a1.mrtl_stts_cd /*婚姻状况代码*/
          ,a1.mrtl_stts /*婚姻状况*/
          ,a2.if_rsk_asss_effc  /*风险测评是否有效*/
          ,a2.rsk_asss_clss_cd	/*风险测评等级代码*/
          ,a2.rsk_asss_clss	   /*风险测评等级*/
          ,a1.if_accp_mkt_advr  /*是否接受营销广告*/
          ,a1.if_blck_cstm /*是否黑名单客户*/
          /*,COALESCE(a6.if_intr_rltn,'N') if_intr_empl*/ /*是否内部员工*/
          /*,COALESCE(a7.if_intr_rltn,'N') if_intr_empl_fmly *//*是否内部员工家属*/
          ,COALESCE(a5.if_upld_id_pht,'N') if_upld_id_pht  /*是否完成证件照片上传*/
          ,a1.entr_wx_add_stt_cd  /*企业微信添加状态代码*/
          ,a1.entr_wx_add_stt  /*企业微信添加状态*/
          ,a1.offl_actv_stts /*线下活动参与情况*/
          ,a1.cstm_18_id_crd_nmbr
          ,a1.cstm_eng_fmly_nm
          ,a1.cstm_eng_nm
          ,a1.cstm_addr_dstr
          ,a1.sppr_id_crtf_fl_typ_cd
          ,a1.sppr_id_crtf_fl_typ
          ,a1.sppr_id_crtf_fl_nmbr
          ,a1.sppr_id_crtf_fl_avlb_prd
          ,a1.fax
          ,a1.cstm_yr_incm
          ,a1.fmly_nmbr_of_ppl
          ,a1.crr_cd
          ,a1.crr
          ,a1.orgn_typ_cd
          ,a1.orgn_typ
          ,a1.orgn_rgst_cptl
          ,a1.orgn_empl_nmbr_of_ppl
          ,a1.orgn_estb_tm
          ,a1.orgn_lgl_entt_nm
          ,a1.orgn_lgl_entt_crtf_nmbr
          ,a1.orgn_lgl_entt_avlb_prd
          ,a1.orgn_orgn_id
          ,a1.orgn_inds_cd
          ,a1.orgn_inds
          ,a1.orgn_mngm_rng
          /*,COALESCE(a6.empl_srrg_key,a7.empl_srrg_key,a8.empl_srrg_key) empl_srrg_key*/ /*员工代理键（员工KMID）*/
          /*,COALESCE(a6.assc_intr_empl_rltn_cd,a7.assc_intr_empl_rltn_cd)	assc_intr_empl_rltn_cd */ /*关联内部员工关系代码*/
          /*,COALESCE(a6.assc_intr_empl_rltn,a7.assc_intr_empl_rltn)	assc_intr_empl_rltn */ /*关联内部员工关系*/
          ,a1.admn_rgn_srrg_key
          ,a1.cstm_id_crd_nmbr_x
FROM  tmp_dim_cstm_dmns_h_cstm_results_x01 a1
LEFT JOIN tmp_dim_cstm_dmns_h_if_rsk_asss_effc a2
ON a1.cstm_crtf_typ_cd = a2.cert_type AND
           a1.cstm_typ_cd = a2.cstm_type AND
           a1.cstm_id_crd_nmbr_x = a2.cert_no AND
           a1.cstm_fll_nm = a2.cust_name
LEFT JOIN tmp_dim_cstm_dmns_h_if_upld_id_pht a5
        ON a1.cstm_crtf_typ_cd = a5.cert_type AND
           a1.cstm_typ_cd = a5.cstm_type AND
           a1.cstm_id_crd_nmbr_x = a5.cert_no AND
           a1.cstm_fll_nm = a5.cust_name
)ORDER BY cstm_id_crd_nmbr_x,cstm_fll_nm
SEGMENTED BY hash(
tmp_dim_cstm_dmns_h_cstm_results_x02.cstm_id_crd_nmbr_x
,tmp_dim_cstm_dmns_h_cstm_results_x02.cstm_fll_nm
) 
ALL NODES KSAFE 1;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_cstm_results_x02'); 


/*加载数据到 可变临时表 dim_cstm_dmns_h_temp*/
INSERT /*+direct*/
INTO tmp_dim_cstm_dmns_h
    (cstm_srrg_key /*客户代理键（客户号）*/
    ,cstm_id /*客户号*/
    ,aftr_mrg_cstm_id /*归并后客户号*/
    ,cstm_typ_cd /*客户类型代码*/
    ,cstm_typ /*客户类型*/
    ,cstm_fll_nm /*客户全称*/
    ,cstm_abbr /*客户简称*/
    ,cstm_crtf_typ_cd /*客户证件类型代码*/
    ,cstm_crtf_typ /*客户证件类型*/
    ,cstm_crtf_nmbr /*客户证件号码*/
    ,cstm_crtf_avlb_prd /*客户证件有效期*/
    ,cstm_addr_cntr /*客户地址_国家*/
    ,cstm_addr_prov /*客户地址_省*/
    ,cstm_addr_cty /*客户地址_市*/
    ,cstm_addr_dtld /*客户地址_详细*/
    ,cstm_zip /*客户邮编*/
    ,cstm_rsk_clss_cd /*客户风险等级代码*/
    ,cstm_rsk_clss /*客户风险等级*/
    ,gndr_cd /*性别代码*/
    ,gndr /*性别*/
    ,brth /*出生日期*/
    ,phn /*移动电话*/
    ,tlph /*固话*/
    ,offc_phn /*办公电话*/
    ,eml /*电子邮箱*/
    ,wrk_inst /*工作单位*/
    ,cstm_stt_cd /*客户状态代码*/
    ,cstm_stt /*客户状态*/
    ,cstm_effc_flg_cd /*客户有效标志代码*/
    ,cstm_effc_flg /*客户有效标志*/
    ,dplm_cd /*学历代码*/
    ,dplm /*学历*/
    ,fmly_asst /*家庭资产*/
    ,mrtl_stts_cd /*婚姻状况代码*/
    ,mrtl_stts /*婚姻状况*/
    ,if_rsk_asss_effc /*风险测评是否有效*/
    ,rsk_asss_clss_cd	/*风险测评等级代码*/
    ,rsk_asss_clss	   /*风险测评等级*/
    ,if_accp_mkt_advr /*是否接受营销广告*/
    ,if_blck_cstm /*是否黑名单客户*/
    ,if_upld_id_pht /*是否完成证件照片上传*/
    ,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
    ,entr_wx_add_stt	/*企业微信添加状态*/
    ,offl_actv_stts /*线下活动参与情况*/
    ,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
    ,cstm_eng_fmly_nm /*客户英文姓氏*/
   ,cstm_eng_nm /*客户英文名称*/
   ,cstm_addr_dstr /*客户地址_区*/
   ,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
   ,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
   ,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
   ,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
   ,fax /*传真*/
   ,cstm_yr_incm /*客户年收入*/
   ,fmly_nmbr_of_ppl /*家庭人数*/
   ,crr_cd /*职业代码*/
   ,crr /*职业*/
   ,orgn_typ_cd /*机构类型代码*/
   ,orgn_typ /*机构类型*/
   ,orgn_rgst_cptl /*机构注册资本*/
   ,orgn_empl_nmbr_of_ppl /*机构员工人数*/
   ,orgn_estb_tm /*机构成立时间*/
   ,orgn_lgl_entt_nm /*机构法人姓名*/
   ,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
   ,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
   ,orgn_orgn_id /*组织机构编号*/
   ,orgn_inds_cd /*机构行业代码*/
   ,orgn_inds /*机构行业*/
   ,orgn_mngm_rng /*机构经营范围*/
   ,admn_rgn_srrg_key /*行政区划代理键*/
    ,etl_btch_dt /*批量日期*/
    ,etl_src_tbl_nm /*源表名*/
    ,etl_job_nm /*加工作业名*/
    ,etl_ld_tm /*加工时间*/)
SELECT 
a1.cstm_srrg_key
,a1.custid
,a1.aftr_mrg_cstm_id
,a1.cstm_typ_cd
,a1.cstm_typ
,a1.cstm_fll_nm
,a1.cstm_abbr
,a1.cstm_crtf_typ_cd
,a1.cstm_crtf_typ
,a1.cstm_crtf_nmbr
,a1.cstm_crtf_avlb_prd
,a1.cstm_addr_cntr
,a1.cstm_addr_prov
,a1.cstm_addr_cty
,a1.cstm_addr_dtld
,a1.cstm_zip
,a1.cstm_rsk_clss_cd
,a1.cstm_rsk_clss
,a1.gndr_cd
,a1.gndr
,a1.brth
,a1.phn
,a1.tlph
,a1.offc_phn
,a1.eml
,a1.wrk_inst
,a1.cstm_stt_cd
,a1.cstm_stt
,a1.cstm_effc_flg_cd
,a1.cstm_effc_flg
,a1.dplm_cd
,a1.dplm
,a1.fmly_asst
,a1.mrtl_stts_cd
,a1.mrtl_stts
,a1.if_rsk_asss_effc
,a1.rsk_asss_clss_cd
,a1.rsk_asss_clss
,a1.if_accp_mkt_advr
,a1.if_blck_cstm
,a1.if_upld_id_pht
,a1.entr_wx_add_stt_cd
,a1.entr_wx_add_stt
,a1.offl_actv_stts
,a1.cstm_18_id_crd_nmbr
,a1.cstm_eng_fmly_nm
,a1.cstm_eng_nm
,a1.cstm_addr_dstr
,a1.sppr_id_crtf_fl_typ_cd
,a1.sppr_id_crtf_fl_typ
,a1.sppr_id_crtf_fl_nmbr
,a1.sppr_id_crtf_fl_avlb_prd
,a1.fax
,a1.cstm_yr_incm
,a1.fmly_nmbr_of_ppl
,a1.crr_cd
,a1.crr
,a1.orgn_typ_cd
,a1.orgn_typ
,a1.orgn_rgst_cptl
,a1.orgn_empl_nmbr_of_ppl
,a1.orgn_estb_tm
,a1.orgn_lgl_entt_nm
,a1.orgn_lgl_entt_crtf_nmbr
,a1.orgn_lgl_entt_avlb_prd
,a1.orgn_orgn_id
,a1.orgn_inds_cd
,a1.orgn_inds
,a1.orgn_mngm_rng
,a1.admn_rgn_srrg_key
,TO_DATE('${TX_DATE_I}', 'YYYYMMDD') etl_btch_dt
,'s054_ecif_vwcust_f' etl_src_tbl_nm
,'tran_cdm_dim_cstm_dmns_h.sql' etl_job_nm  /*写死*/
,SYSDATE etl_ld_tm
FROM  tmp_dim_cstm_dmns_h_cstm_results_x02 a1;

/*开始拉链*/

DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_cur CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_cur ON COMMIT PRESERVE ROWS
AS (SELECT cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
FROM ${CDM_SCHEMA}.dim_cstm_dmns_h  
   WHERE 1<>1
)
ORDER BY 
    cstm_srrg_key
   ,etl_md5
   SEGMENTED BY hash(tmp_dim_cstm_dmns_h_cur.cstm_srrg_key,tmp_dim_cstm_dmns_h_cur.etl_md5) 
ALL NODES KSAFE 1;
   ;



DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_pre CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_pre ON COMMIT PRESERVE ROWS
AS (SELECT cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
FROM ${CDM_SCHEMA}.dim_cstm_dmns_h  
   WHERE 1<>1
)
ORDER BY 
    cstm_srrg_key
   ,etl_md5
   SEGMENTED BY hash(tmp_dim_cstm_dmns_h_pre.cstm_srrg_key,tmp_dim_cstm_dmns_h_pre.etl_md5) 
ALL NODES KSAFE 1;
   ;

DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_del CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_del ON COMMIT PRESERVE ROWS
AS (SELECT cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
FROM ${CDM_SCHEMA}.dim_cstm_dmns_h  
   WHERE 1<>1
)
ORDER BY 
    cstm_srrg_key
   ,etl_md5
   SEGMENTED BY hash(tmp_dim_cstm_dmns_h_del.cstm_srrg_key,tmp_dim_cstm_dmns_h_del.etl_md5) 
ALL NODES KSAFE 1;
   ;

DROP TABLE IF EXISTS tmp_dim_cstm_dmns_h_ins CASCADE;
CREATE LOCAL TEMP TABLE tmp_dim_cstm_dmns_h_ins ON COMMIT PRESERVE ROWS
AS (SELECT cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
FROM ${CDM_SCHEMA}.dim_cstm_dmns_h  
   WHERE 1<>1
   )
ORDER BY 
    cstm_srrg_key
   ,etl_md5
   SEGMENTED BY hash(tmp_dim_cstm_dmns_h_ins.cstm_srrg_key,tmp_dim_cstm_dmns_h_ins.etl_md5) 
ALL NODES KSAFE 1;
   ;



/*加载数据到表中*/
INSERT /*+direct*/
  INTO tmp_dim_cstm_dmns_h_cur
(cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/)     
SELECT 
cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,TO_DATE('${TX_DATE_I}', 'YYYYMMDD')  strt_dt /*开始日期*/
,TO_DATE('${MAXDATE}', 'YYYYMMDD')  end_dt  /*结束日期*/
,MD5(COALESCE(cstm_srrg_key,'')
||'+'||COALESCE(cstm_id,'')
||'+'||COALESCE(aftr_mrg_cstm_id,'')
||'+'||COALESCE(cstm_typ_cd,'')
||'+'||COALESCE(cstm_typ,'')
||'+'||COALESCE(cstm_fll_nm,'')
||'+'||COALESCE(cstm_abbr,'')
||'+'||COALESCE(cstm_crtf_typ_cd,'')
||'+'||COALESCE(cstm_crtf_typ,'')
||'+'||COALESCE(cstm_crtf_nmbr,'')
||'+'||COALESCE(TO_CHAR(cstm_crtf_avlb_prd,'YYYYMMDD'),'')
||'+'||COALESCE(cstm_addr_cntr,'')
||'+'||COALESCE(cstm_addr_prov,'')
||'+'||COALESCE(cstm_addr_cty,'')
||'+'||COALESCE(cstm_addr_dtld,'')
||'+'||COALESCE(cstm_zip,'')
||'+'||COALESCE(cstm_rsk_clss_cd,'')
||'+'||COALESCE(cstm_rsk_clss,'')
||'+'||COALESCE(gndr_cd,'')
||'+'||COALESCE(gndr,'')
||'+'||COALESCE(TO_CHAR(brth,'YYYYMMDD'),'')
||'+'||COALESCE(phn,'')
||'+'||COALESCE(tlph,'')
||'+'||COALESCE(offc_phn,'')
||'+'||COALESCE(eml,'')
||'+'||COALESCE(wrk_inst,'')
||'+'||COALESCE(cstm_stt_cd,'')
||'+'||COALESCE(cstm_stt,'')
||'+'||COALESCE(cstm_effc_flg_cd,'')
||'+'||COALESCE(cstm_effc_flg,'')
||'+'||COALESCE(dplm_cd,'')
||'+'||COALESCE(dplm,'')
||'+'||COALESCE(TO_CHAR(CAST(TO_CHAR(fmly_asst) AS NUMBER(37,8))),'')
||'+'||COALESCE(mrtl_stts_cd,'')
||'+'||COALESCE(mrtl_stts,'')
||'+'||COALESCE(if_rsk_asss_effc,'')
||'+'||COALESCE(rsk_asss_clss_cd,'')
||'+'||COALESCE(rsk_asss_clss,'')
||'+'||COALESCE(if_accp_mkt_advr,'')
||'+'||COALESCE(if_blck_cstm,'')
||'+'||COALESCE(if_upld_id_pht,'')
||'+'||COALESCE(entr_wx_add_stt_cd	,'')
||'+'||COALESCE(entr_wx_add_stt,'')
||'+'||COALESCE(offl_actv_stts,'')
||'+'||COALESCE(cstm_18_id_crd_nmbr,'')
||'+'||COALESCE(cstm_eng_fmly_nm,'')
||'+'||COALESCE(cstm_eng_nm,'')
||'+'||COALESCE(cstm_addr_dstr,'')
||'+'||COALESCE(sppr_id_crtf_fl_typ_cd,'')
||'+'||COALESCE(sppr_id_crtf_fl_typ,'')
||'+'||COALESCE(sppr_id_crtf_fl_nmbr,'')
||'+'||COALESCE(TO_CHAR(sppr_id_crtf_fl_avlb_prd,'YYYYMMDD'),'')
||'+'||COALESCE(fax,'')
||'+'||COALESCE(TO_CHAR(CAST(TO_CHAR(cstm_yr_incm) AS NUMBER(37,8))),'')
||'+'||COALESCE(admn_rgn_srrg_key,'')
||'+'||COALESCE(TO_CHAR(CAST(TO_CHAR(fmly_nmbr_of_ppl) AS NUMBER(18,0))),'')
||'+'||COALESCE(crr_cd,'')
||'+'||COALESCE(crr,'')
||'+'||COALESCE(orgn_typ_cd,'')
||'+'||COALESCE(orgn_typ,'')
||'+'||COALESCE(TO_CHAR(CAST(TO_CHAR(orgn_rgst_cptl) AS NUMBER(18,0))),'') 
||'+'||COALESCE(TO_CHAR(CAST(TO_CHAR(orgn_empl_nmbr_of_ppl) AS NUMBER(18,0))),'')
||'+'||COALESCE(TO_CHAR(orgn_estb_tm,'YYYYMMDD'),'')
||'+'||COALESCE(orgn_lgl_entt_nm,'')
||'+'||COALESCE(orgn_lgl_entt_crtf_nmbr,'')
||'+'||COALESCE(TO_CHAR(orgn_lgl_entt_avlb_prd,'YYYYMMDD'),'')
||'+'||COALESCE(orgn_orgn_id,'')
||'+'||COALESCE(orgn_inds_cd,'')
||'+'||COALESCE(orgn_inds,'')
||'+'||COALESCE(orgn_mngm_rng,'')
) etl_md5
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
FROM tmp_dim_cstm_dmns_h
;
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_cur');


DELETE /*+ direct */ FROM  ${CDM_SCHEMA}.dim_cstm_dmns_h 
            WHERE strt_dt >= TO_DATE('${TX_DATE_I}','YYYYMMDD'); 

UPDATE /*+ direct */  ${CDM_SCHEMA}.dim_cstm_dmns_h 
            SET    end_dt = TO_DATE('${MAXDATE}','YYYYMMDD')
            WHERE  end_dt >= TO_DATE('${TX_DATE_I}','YYYYMMDD')  AND
                   end_dt <> TO_DATE('${MAXDATE}','YYYYMMDD')
             ;

/*把当前有效数据插入*/
INSERT /*+ direct */ INTO  tmp_dim_cstm_dmns_h_pre (
   cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
)
 SELECT   
 cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
FROM ${CDM_SCHEMA}.dim_cstm_dmns_h
WHERE end_dt = TO_DATE('${MAXDATE}','YYYYMMDD');


SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_pre');


/*今日与前一天有效全量数据,通过全字段进行比较,找出新增及更新的数据*/
INSERT /*+ direct */ INTO  tmp_dim_cstm_dmns_h_ins (
 cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
)
 SELECT 
 a1.cstm_srrg_key /*客户代理键（客户号）*/
,a1.cstm_id /*客户号*/
,a1.aftr_mrg_cstm_id /*归并后客户号*/
,a1.cstm_typ_cd /*客户类型代码*/
,a1.cstm_typ /*客户类型*/
,a1.cstm_fll_nm /*客户全称*/
,a1.cstm_abbr /*客户简称*/
,a1.cstm_crtf_typ_cd /*客户证件类型代码*/
,a1.cstm_crtf_typ /*客户证件类型*/
,a1.cstm_crtf_nmbr /*客户证件号码*/
,a1.cstm_crtf_avlb_prd /*客户证件有效期*/
,a1.cstm_addr_cntr /*客户地址_国家*/
,a1.cstm_addr_prov /*客户地址_省*/
,a1.cstm_addr_cty /*客户地址_市*/
,a1.cstm_addr_dtld /*客户地址_详细*/
,a1.cstm_zip /*客户邮编*/
,a1.cstm_rsk_clss_cd /*客户风险等级代码*/
,a1.cstm_rsk_clss /*客户风险等级*/
,a1.gndr_cd /*性别代码*/
,a1.gndr /*性别*/
,a1.brth /*出生日期*/
,a1.phn /*移动电话*/
,a1.tlph /*固话*/
,a1.offc_phn /*办公电话*/
,a1.eml /*电子邮箱*/
,a1.wrk_inst /*工作单位*/
,a1.cstm_stt_cd /*客户状态代码*/
,a1.cstm_stt /*客户状态*/
,a1.cstm_effc_flg_cd /*客户有效标志代码*/
,a1.cstm_effc_flg   /*客户有效标志*/
,a1.dplm_cd         /*学历代码*/
,a1.dplm           /*学历*/
,a1.fmly_asst     /*家庭资产*/
,a1.mrtl_stts_cd  /*婚姻状况代码*/
,a1.mrtl_stts     /*婚姻状况*/
,a1.if_rsk_asss_effc /*风险测评是否有效*/
,a1.rsk_asss_clss_cd	/*风险测评等级代码*/
,a1.rsk_asss_clss	   /*风险测评等级*/
,a1.if_accp_mkt_advr /*是否接受营销广告*/
,a1.if_blck_cstm /*是否黑名单客户*/
,a1.if_upld_id_pht /*是否完成证件照片上传*/
,a1.entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,a1.entr_wx_add_stt	/*企业微信添加状态*/
,a1.offl_actv_stts /*线下活动参与情况*/
,a1.cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,a1.cstm_eng_fmly_nm /*客户英文姓氏*/
,a1.cstm_eng_nm /*客户英文名称*/
,a1.cstm_addr_dstr /*客户地址_区*/
,a1.sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,a1.sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,a1.sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,a1.sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,a1.fax /*传真*/
,a1.cstm_yr_incm /*客户年收入*/
,a1.admn_rgn_srrg_key /*行政区划代理键*/
,a1.fmly_nmbr_of_ppl /*家庭人数*/
,a1.crr_cd /*职业代码*/
,a1.crr /*职业*/
,a1.orgn_typ_cd /*机构类型代码*/
,a1.orgn_typ /*机构类型*/
,a1.orgn_rgst_cptl /*机构注册资本*/
,a1.orgn_empl_nmbr_of_ppl /*机构员工人数*/
,a1.orgn_estb_tm /*机构成立时间*/
,a1.orgn_lgl_entt_nm /*机构法人姓名*/
,a1.orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,a1.orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,a1.orgn_orgn_id /*组织机构编号*/
,a1.orgn_inds_cd /*机构行业代码*/
,a1.orgn_inds /*机构行业*/
,a1.orgn_mngm_rng /*机构经营范围*/
,a1.strt_dt /*开始日期*/
,TO_DATE('${MAXDATE}','YYYYMMDD')   end_dt /* 结束日期 */
,a1.etl_md5 /*Md5值*/
,a1.etl_btch_dt /*批量日期*/
,a1.etl_src_tbl_nm /*源表名*/
,a1.etl_job_nm /*加工作业名*/
,a1.etl_ld_tm /*加工时间*/
 FROM tmp_dim_cstm_dmns_h_cur  a1
   WHERE   NOT EXISTS
 (
 SELECT 1 x FROM  tmp_dim_cstm_dmns_h_pre  a2   
 WHERE a1.etl_md5 = a2.etl_md5
 );
 
 SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_ins');
 
 
/*前一天与今日有效全量数据,通过主键进行比较,找出今日删除的数据并封口*/
INSERT /*+ direct */ INTO  tmp_dim_cstm_dmns_h_del (
 cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
)
SELECT   
 a1.cstm_srrg_key /*客户代理键（客户号）*/
,a1.cstm_id /*客户号*/
,a1.aftr_mrg_cstm_id /*归并后客户号*/
,a1.cstm_typ_cd /*客户类型代码*/
,a1.cstm_typ /*客户类型*/
,a1.cstm_fll_nm /*客户全称*/
,a1.cstm_abbr /*客户简称*/
,a1.cstm_crtf_typ_cd /*客户证件类型代码*/
,a1.cstm_crtf_typ /*客户证件类型*/
,a1.cstm_crtf_nmbr /*客户证件号码*/
,a1.cstm_crtf_avlb_prd /*客户证件有效期*/
,a1.cstm_addr_cntr /*客户地址_国家*/
,a1.cstm_addr_prov /*客户地址_省*/
,a1.cstm_addr_cty /*客户地址_市*/
,a1.cstm_addr_dtld /*客户地址_详细*/
,a1.cstm_zip /*客户邮编*/
,a1.cstm_rsk_clss_cd /*客户风险等级代码*/
,a1.cstm_rsk_clss /*客户风险等级*/
,a1.gndr_cd /*性别代码*/
,a1.gndr /*性别*/
,a1.brth /*出生日期*/
,a1.phn /*移动电话*/
,a1.tlph /*固话*/
,a1.offc_phn /*办公电话*/
,a1.eml /*电子邮箱*/
,a1.wrk_inst /*工作单位*/
,a1.cstm_stt_cd /*客户状态代码*/
,a1.cstm_stt /*客户状态*/
,a1.cstm_effc_flg_cd /*客户有效标志代码*/
,a1.cstm_effc_flg   /*客户有效标志*/
,a1.dplm_cd         /*学历代码*/
,a1.dplm           /*学历*/
,a1.fmly_asst     /*家庭资产*/
,a1.mrtl_stts_cd  /*婚姻状况代码*/
,a1.mrtl_stts     /*婚姻状况*/
,a1.if_rsk_asss_effc /*风险测评是否有效*/
,a1.rsk_asss_clss_cd	/*风险测评等级代码*/
,a1.rsk_asss_clss	   /*风险测评等级*/
,a1.if_accp_mkt_advr /*是否接受营销广告*/
,a1.if_blck_cstm /*是否黑名单客户*/
,a1.if_upld_id_pht /*是否完成证件照片上传*/
,a1.entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,a1.entr_wx_add_stt	/*企业微信添加状态*/
,a1.offl_actv_stts /*线下活动参与情况*/
,a1.cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,a1.cstm_eng_fmly_nm /*客户英文姓氏*/
,a1.cstm_eng_nm /*客户英文名称*/
,a1.cstm_addr_dstr /*客户地址_区*/
,a1.sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,a1.sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,a1.sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,a1.sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,a1.fax /*传真*/
,a1.cstm_yr_incm /*客户年收入*/
,a1.admn_rgn_srrg_key /*行政区划代理键*/
,a1.fmly_nmbr_of_ppl /*家庭人数*/
,a1.crr_cd /*职业代码*/
,a1.crr /*职业*/
,a1.orgn_typ_cd /*机构类型代码*/
,a1.orgn_typ /*机构类型*/
,a1.orgn_rgst_cptl /*机构注册资本*/
,a1.orgn_empl_nmbr_of_ppl /*机构员工人数*/
,a1.orgn_estb_tm /*机构成立时间*/
,a1.orgn_lgl_entt_nm /*机构法人姓名*/
,a1.orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,a1.orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,a1.orgn_orgn_id /*组织机构编号*/
,a1.orgn_inds_cd /*机构行业代码*/
,a1.orgn_inds /*机构行业*/
,a1.orgn_mngm_rng /*机构经营范围*/
,a1.strt_dt /*开始日期*/
,TO_DATE('${TX_DATE_I}','YYYYMMDD')   end_dt /* 结束日期 */
,a1.etl_md5 /*Md5值*/
,a1.etl_btch_dt /*批量日期*/
,a1.etl_src_tbl_nm /*源表名*/
,a1.etl_job_nm /*加工作业名*/
,a1.etl_ld_tm /*加工时间*/
 FROM tmp_dim_cstm_dmns_h_pre  a1
   WHERE   NOT EXISTS 
 (   
 SELECT 1 x FROM  tmp_dim_cstm_dmns_h_cur  a2   
 WHERE 
 a1.cstm_srrg_key = a2.cstm_srrg_key
 )   
 ;
 
 
 /*前一天与今日有效全量数据,通过全字段进行比较,找出更新的数据并封口*/
INSERT /*+ direct */ INTO  tmp_dim_cstm_dmns_h_del (
cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
)
 SELECT   
 a1.cstm_srrg_key /*客户代理键（客户号）*/
,a1.cstm_id /*客户号*/
,a1.aftr_mrg_cstm_id /*归并后客户号*/
,a1.cstm_typ_cd /*客户类型代码*/
,a1.cstm_typ /*客户类型*/
,a1.cstm_fll_nm /*客户全称*/
,a1.cstm_abbr /*客户简称*/
,a1.cstm_crtf_typ_cd /*客户证件类型代码*/
,a1.cstm_crtf_typ /*客户证件类型*/
,a1.cstm_crtf_nmbr /*客户证件号码*/
,a1.cstm_crtf_avlb_prd /*客户证件有效期*/
,a1.cstm_addr_cntr /*客户地址_国家*/
,a1.cstm_addr_prov /*客户地址_省*/
,a1.cstm_addr_cty /*客户地址_市*/
,a1.cstm_addr_dtld /*客户地址_详细*/
,a1.cstm_zip /*客户邮编*/
,a1.cstm_rsk_clss_cd /*客户风险等级代码*/
,a1.cstm_rsk_clss /*客户风险等级*/
,a1.gndr_cd /*性别代码*/
,a1.gndr /*性别*/
,a1.brth /*出生日期*/
,a1.phn /*移动电话*/
,a1.tlph /*固话*/
,a1.offc_phn /*办公电话*/
,a1.eml /*电子邮箱*/
,a1.wrk_inst /*工作单位*/
,a1.cstm_stt_cd /*客户状态代码*/
,a1.cstm_stt /*客户状态*/
,a1.cstm_effc_flg_cd /*客户有效标志代码*/
,a1.cstm_effc_flg   /*客户有效标志*/
,a1.dplm_cd         /*学历代码*/
,a1.dplm           /*学历*/
,a1.fmly_asst     /*家庭资产*/
,a1.mrtl_stts_cd  /*婚姻状况代码*/
,a1.mrtl_stts     /*婚姻状况*/
,a1.if_rsk_asss_effc /*风险测评是否有效*/
,a1.rsk_asss_clss_cd	/*风险测评等级代码*/
,a1.rsk_asss_clss	   /*风险测评等级*/
,a1.if_accp_mkt_advr /*是否接受营销广告*/
,a1.if_blck_cstm /*是否黑名单客户*/
,a1.if_upld_id_pht /*是否完成证件照片上传*/
,a1.entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,a1.entr_wx_add_stt	/*企业微信添加状态*/
,a1.offl_actv_stts /*线下活动参与情况*/
,a1.cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,a1.cstm_eng_fmly_nm /*客户英文姓氏*/
,a1.cstm_eng_nm /*客户英文名称*/
,a1.cstm_addr_dstr /*客户地址_区*/
,a1.sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,a1.sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,a1.sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,a1.sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,a1.fax /*传真*/
,a1.cstm_yr_incm /*客户年收入*/
,a1.admn_rgn_srrg_key /*行政区划代理键*/
,a1.fmly_nmbr_of_ppl /*家庭人数*/
,a1.crr_cd /*职业代码*/
,a1.crr /*职业*/
,a1.orgn_typ_cd /*机构类型代码*/
,a1.orgn_typ /*机构类型*/
,a1.orgn_rgst_cptl /*机构注册资本*/
,a1.orgn_empl_nmbr_of_ppl /*机构员工人数*/
,a1.orgn_estb_tm /*机构成立时间*/
,a1.orgn_lgl_entt_nm /*机构法人姓名*/
,a1.orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,a1.orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,a1.orgn_orgn_id /*组织机构编号*/
,a1.orgn_inds_cd /*机构行业代码*/
,a1.orgn_inds /*机构行业*/
,a1.orgn_mngm_rng /*机构经营范围*/
,a1.strt_dt /*开始日期*/
,TO_DATE('${TX_DATE_I}','YYYYMMDD')   end_dt /* 结束日期 */
,a1.etl_md5 /*Md5值*/
,a1.etl_btch_dt /*批量日期*/
,a1.etl_src_tbl_nm /*源表名*/
,a1.etl_job_nm /*加工作业名*/
,a1.etl_ld_tm /*加工时间*/ 
FROM tmp_dim_cstm_dmns_h_pre  a1
   WHERE   NOT EXISTS 
 (   
 SELECT 1 x FROM  tmp_dim_cstm_dmns_h_cur  a2  
 WHERE a1.etl_md5 = a2.etl_md5

 )   
 AND  NOT EXISTS  
 (  
 SELECT 1 x FROM  tmp_dim_cstm_dmns_h_del  a3 
 WHERE a1.cstm_srrg_key = a3.cstm_srrg_key
 )   
 ;
 
SELECT ANALYZE_STATISTICS('tmp_dim_cstm_dmns_h_del'); 
 
/*主键对比，删除目标表中在DEL表中的数据*/
 DELETE /*+ direct */ FROM  ${CDM_SCHEMA}.dim_cstm_dmns_h
   WHERE  EXISTS 
 (  
 SELECT 1 x FROM  tmp_dim_cstm_dmns_h_del  a2  
 WHERE 
 ${CDM_SCHEMA}.dim_cstm_dmns_h.cstm_srrg_key = a2.cstm_srrg_key
AND ${CDM_SCHEMA}.dim_cstm_dmns_h.strt_dt = a2.strt_dt
);

 /*将DEL的数据插入到目标表中*/
 INSERT /*+ direct */ INTO  ${CDM_SCHEMA}.dim_cstm_dmns_h(
cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
)
 SELECT   
cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,TO_DATE('${TX_DATE_BATCH}', 'YYYYMMDD')                     /*批量日期*/ 
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,SYSDATE etl_ld_tm /*加工时间*/
FROM tmp_dim_cstm_dmns_h_del 
;
 

 /*将INS的数据插入到目标表中*/
INSERT /*+ direct */ INTO  ${CDM_SCHEMA}.dim_cstm_dmns_h(
cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,etl_ld_tm /*加工时间*/
)
 SELECT   
cstm_srrg_key /*客户代理键（客户号）*/
,cstm_id /*客户号*/
,aftr_mrg_cstm_id /*归并后客户号*/
,cstm_typ_cd /*客户类型代码*/
,cstm_typ /*客户类型*/
,cstm_fll_nm /*客户全称*/
,cstm_abbr /*客户简称*/
,cstm_crtf_typ_cd /*客户证件类型代码*/
,cstm_crtf_typ /*客户证件类型*/
,cstm_crtf_nmbr /*客户证件号码*/
,cstm_crtf_avlb_prd /*客户证件有效期*/
,cstm_addr_cntr /*客户地址_国家*/
,cstm_addr_prov /*客户地址_省*/
,cstm_addr_cty /*客户地址_市*/
,cstm_addr_dtld /*客户地址_详细*/
,cstm_zip /*客户邮编*/
,cstm_rsk_clss_cd /*客户风险等级代码*/
,cstm_rsk_clss /*客户风险等级*/
,gndr_cd /*性别代码*/
,gndr /*性别*/
,brth /*出生日期*/
,phn /*移动电话*/
,tlph /*固话*/
,offc_phn /*办公电话*/
,eml /*电子邮箱*/
,wrk_inst /*工作单位*/
,cstm_stt_cd /*客户状态代码*/
,cstm_stt /*客户状态*/
,cstm_effc_flg_cd /*客户有效标志代码*/
,cstm_effc_flg   /*客户有效标志*/
,dplm_cd         /*学历代码*/
,dplm           /*学历*/
,fmly_asst     /*家庭资产*/
,mrtl_stts_cd  /*婚姻状况代码*/
,mrtl_stts     /*婚姻状况*/
,if_rsk_asss_effc /*风险测评是否有效*/
,rsk_asss_clss_cd	/*风险测评等级代码*/
,rsk_asss_clss	   /*风险测评等级*/
,if_accp_mkt_advr /*是否接受营销广告*/
,if_blck_cstm /*是否黑名单客户*/
,if_upld_id_pht /*是否完成证件照片上传*/
,entr_wx_add_stt_cd	/*企业微信添加状态代码*/
,entr_wx_add_stt	/*企业微信添加状态*/
,offl_actv_stts /*线下活动参与情况*/
,cstm_18_id_crd_nmbr /*客户18位身份证号码*/
,cstm_eng_fmly_nm /*客户英文姓氏*/
,cstm_eng_nm /*客户英文名称*/
,cstm_addr_dstr /*客户地址_区*/
,sppr_id_crtf_fl_typ_cd /*辅助身份证明文件类型代码*/
,sppr_id_crtf_fl_typ /*辅助身份证明文件类型*/
,sppr_id_crtf_fl_nmbr /*辅助身份证明文件号码*/
,sppr_id_crtf_fl_avlb_prd /*辅助身份证明文件有效期*/
,fax /*传真*/
,cstm_yr_incm /*客户年收入*/
,admn_rgn_srrg_key /*行政区划代理键*/
,fmly_nmbr_of_ppl /*家庭人数*/
,crr_cd /*职业代码*/
,crr /*职业*/
,orgn_typ_cd /*机构类型代码*/
,orgn_typ /*机构类型*/
,orgn_rgst_cptl /*机构注册资本*/
,orgn_empl_nmbr_of_ppl /*机构员工人数*/
,orgn_estb_tm /*机构成立时间*/
,orgn_lgl_entt_nm /*机构法人姓名*/
,orgn_lgl_entt_crtf_nmbr /*机构法人证件号码*/
,orgn_lgl_entt_avlb_prd /*机构法人证件有效期*/
,orgn_orgn_id /*组织机构编号*/
,orgn_inds_cd /*机构行业代码*/
,orgn_inds /*机构行业*/
,orgn_mngm_rng /*机构经营范围*/
,strt_dt /*开始日期*/
,end_dt /*结束日期*/
,etl_md5 /*Md5值*/
,TO_DATE('${TX_DATE_BATCH}', 'YYYYMMDD') etl_btch_dt /*批量日期*/
,etl_src_tbl_nm /*源表名*/
,etl_job_nm /*加工作业名*/
,SYSDATE etl_ld_tm /*加工时间*/
FROM tmp_dim_cstm_dmns_h_ins 
 ;
SELECT ANALYZE_STATISTICS('${CDM_SCHEMA}.dim_cstm_dmns_h');
/*-ENDFOR*/