moved {
  from = module.tdr_preingest
  to   = module.preingest["tdr"]
}

moved {
  from = module.pa_preingest
  to   = module.preingest["pa"]
}

moved {
  from = module.dri_preingest
  to   = module.preingest["dri"]
}

moved {
  from = module.ad_hoc_preingest
  to   = module.preingest["ad_hoc"]
}

moved {
  from = module.court_document_preingest
  to   = module.preingest["court_document"]
}

moved {
  from = module.cc_restore_preingest
  to   = module.preingest["cc_restore"]
}
