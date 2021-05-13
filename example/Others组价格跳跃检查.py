from data_manager.DailyDataManager import DailyDataManager

self = DailyDataManager()
close_df = self.get_field(symbol='FB', field='close')

import rqdatac as rq
rq.init('license',
        'blJSw2o4ga7IMPOc-TQ3hZLCbGwAGE1ge6nj5Mj-gjUP-E3Q7Wi5WRy68eRWXIWxOdxB66UPU6uFE9wWe_NYzFHnZCunxcrIsSnT0ZUh5qQJ0u5rucs2WmYhqhaov0jtEFGCmLlnfcaTLOQUJz7Wb1xJ3qBvxXu9pJig4f4j-jg=Gq3WIxDUv98SbZSSegwPQUOg8Lrnz_iAjn2Qy7ZgFZGq8EgUYy6-aaYYhqvp7Ox6O4nuirhlPV6AIfTaVD-d0vD81hZ5Ts1kCTvLTRJjsQIj4-kuZC80RrpOkVYy4THZxRcG9Emt6zWODkl7sby4RJA5W4k_aOIcMmtmNfIZd_s=',
        ("rqdatad-pro.ricequant.com", 16011))

df = rq.get_price(order_book_ids='FB2001',
             start_date='2019-11-15',
             end_date='2019-12-11',
             frequency='1d')