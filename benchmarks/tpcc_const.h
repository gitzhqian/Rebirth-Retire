/**
 * These enum variables are used for table initialization.
 * [ Make the code more readable ]
 */

#if TPCC_SMALL
enum {
    W_ID,
    W_NAME,
    W_STREET_1,
    W_STREET_2,
    W_CITY,
    W_STATE,
    W_ZIP,
    W_TAX,
    W_YTD
};
enum {
    D_ID,
    D_W_ID,
    D_NAME,
    D_STREET_1,
    D_STREET_2,
    D_CITY,
    D_STATE,
    D_ZIP,
    D_TAX,
    D_YTD,
    D_NEXT_O_ID
};
enum {
    C_ID,
    C_D_ID,
    C_W_ID,
    C_MIDDLE,
    C_LAST,
    C_STATE,
    C_CREDIT,
    C_DISCOUNT,
    C_BALANCE,
    C_YTD_PAYMENT,
    C_PAYMENT_CNT
};
enum {
    H_C_ID,
    H_C_D_ID,
    H_C_W_ID,
    H_D_ID,
    H_W_ID,
    H_DATE,
    H_AMOUNT
};
enum {
    NO_O_ID,
    NO_D_ID,
    NO_W_ID
};
enum {
    O_ID,
    O_C_ID,
    O_D_ID,
    O_W_ID,
    O_ENTRY_D,
    O_CARRIER_ID,
    O_OL_CNT,
    O_ALL_LOCAL
};
enum {
    OL_O_ID,
    OL_D_ID,
    OL_W_ID,
    OL_NUMBER,
    OL_I_ID
};
enum {
    I_ID,
    I_IM_ID,
    I_NAME,
    I_PRICE,
    I_DATA
};
enum {
    S_I_ID,
    S_W_ID,
    S_QUANTITY,
    S_REMOTE_CNT
};
#else
enum {
    W_ID,
    W_NAME,
    W_STREET_1,
    W_STREET_2,
    W_CITY,
    W_STATE,
    W_ZIP,
    W_TAX,
    W_YTD
};
enum {
    D_ID,
    D_W_ID,
    D_NAME,
    D_STREET_1,
    D_STREET_2,
    D_CITY,
    D_STATE,
    D_ZIP,
    D_TAX,
    D_YTD,
    D_NEXT_O_ID
};
enum {
    C_ID,
    C_D_ID,
    C_W_ID,
    C_FIRST,
    C_MIDDLE,
    C_LAST,
    C_STREET_1,
    C_STREET_2,
    C_CITY,
    C_STATE,
    C_ZIP,
    C_PHONE,
    C_SINCE,
    C_CREDIT,
    C_CREDIT_LIM,
    C_DISCOUNT,
    C_BALANCE,
    C_YTD_PAYMENT,
    C_PAYMENT_CNT,
    C_DELIVERY_CNT,
    C_DATA
};
enum {
    H_C_ID,
    H_C_D_ID,
    H_C_W_ID,
    H_D_ID,
    H_W_ID,
    H_DATE,
    H_AMOUNT,
    H_DATA
};
enum {
    NO_O_ID,
    NO_D_ID,
    NO_W_ID
};
enum {
    O_ID,
    O_C_ID,
    O_D_ID,
    O_W_ID,
    O_ENTRY_D,
    O_CARRIER_ID,
    O_OL_CNT,
    O_ALL_LOCAL
};
enum {
    OL_O_ID,
    OL_D_ID,
    OL_W_ID,
    OL_NUMBER,
    OL_I_ID,
    OL_SUPPLY_W_ID,
    OL_DELIVERY_D,
    OL_QUANTITY,
    OL_AMOUNT,
    OL_DIST_INFO
};
enum {
    I_ID,
    I_IM_ID,
    I_NAME,
    I_PRICE,
    I_DATA
};
enum {
    S_I_ID,
    S_W_ID,
    S_QUANTITY,
    S_DIST_01,
    S_DIST_02,
    S_DIST_03,
    S_DIST_04,
    S_DIST_05,
    S_DIST_06,
    S_DIST_07,
    S_DIST_08,
    S_DIST_09,
    S_DIST_10,
    S_YTD,
    S_ORDER_CNT,
    S_REMOTE_CNT,
    S_DATA
};


#endif

//struct Region{
//    uint64_t region_id;
//    std::string region_name;
//    std::string region_commont;  //152
//};
//struct Nation{
//    uint64_t nation_id;
//    uint64_t region_id;
//    std::string nation_name;
//    std::string nation_commont;  //152
//};
//struct Supplier{
//    uint64_t su_supp_id;
//    uint64_t su_nation_id;
//    double su_acctbal;
//    std::string su_name;
//    std::string su_address; //40
//    std::string su_phone;  //15
//    std::string su_comment; //15
//};
//
//static const char *regions[] = {"ASIA"  };
//const Nation nations[] = {{3,  0, "CHINA"}
//};
//const Nation nations[] = {{48, 0, "ALGERIA"},
//                          {49, 1, "ARGENTINA"},
//                          {50, 1, "BRAZIL"},
//                          {51, 1, "CANADA"},
//                          {52, 4, "EGYPT" },
//                          {53, 0, "ETHIOPIA"},
//                          {54, 3, "FRANCE"  },
//                          {55, 3, "GERMANY" },
//                          {56, 2, "INDIA"   },
//                          {57, 2, "INDONESIA"},
//                          {65, 4, "IRAN"     },
//                          {66, 4, "IRAQ"     },
//                          {67, 2, "JAPAN"    },
//                          {68, 4, "JORDAN"   },
//                          {69, 0, "KENYA"    },
//                          {70, 0, "MOROCCO"  },
//                          {71, 0, "MOZAMBIQUE"},
//                          {72, 1, "PERU"      },
//                          {73, 2, "CHINA"     },
//                          {74, 3, "ROMANIA"   },
//                          {75, 4, "SAUDI ARABIA"                                },
//                          {76, 2, "VIETNAM"                                     },
//                          {77, 3, "RUSSIA"                                      },
//                          {78, 3, "UNITED KINGDOM"                              },
//                          {79, 1, "UNITED STATES"                               },
//                          {80, 2, "CHINA"                                       },
//                          {81, 2, "PAKISTAN"                                    },
//                          {82, 2, "BANGLADESH"                                  },
//                          {83, 1, "MEXICO"                                      },
//                          {84, 2, "PHILIPPINES"                                 },
//                          {85, 2, "THAILAND"                                    },
//                          {86, 3, "ITALY"                                       },
//                          {87, 0, "SOUTH AFRICA"                                },
//                          {88, 2, "SOUTH KOREA"                                 },
//                          {89, 1, "COLOMBIA"                                    },
//                          {90, 3, "SPAIN"                                       },
//                          {97, 3, "UKRAINE"                                     },
//                          {98, 3, "POLAND"                                      },
//                          {99, 0, "SUDAN"                                       },
//                          {100, 2, "UZBEKISTAN"                                 },
//                          {101, 2, "MALAYSIA"                                   },
//                          {102, 1, "VENEZUELA"                                  },
//                          {103, 2, "NEPAL"                                      },
//                          {104, 2, "AFGHANISTAN"                                },
//                          {105, 2, "NORTH KOREA"                                },
//                          {106, 2, "TAIWAN"                                     },
//                          {107, 0, "GHANA"                                      },
//                          {108, 0, "IVORY COAST"                                },
//                          {109, 4, "SYRIA"                                      },
//                          {110, 0, "MADAGASCAR"                                 },
//                          {111, 0, "CAMEROON"                                   },
//                          {112, 2, "SRI LANKA"                                  },
//                          {113, 3, "ROMANIA"                                    },
//                          {114, 3, "NETHERLANDS"                                },
//                          {115, 2, "CAMBODIA"                                   },
//                          {116, 3, "BELGIUM"                                    },
//                          {117, 3, "GREECE"                                     },
//                          {118, 3, "PORTUGAL"                                   },
//                          {119, 4, "ISRAEL"                                     },
//                          {120, 3, "FINLAND"                                    },
//                          {121, 2, "SINGAPORE"                                  },
//                          {122, 3, "NORWAY"                                     }
//};
//static const char *regions[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

