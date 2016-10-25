#! /usr/bin/python

from checkmateconstant import constant

class CheckMateConfig(object):

    def __init__(self):
        pass

    @constant
    def UTG_PORT():
        #cclib.py - def send_to_utg
        return '295'

    @constant
    def UTG_HOST_IP():
        #cclib.py - def send_to_utg
        return '10.0.0.248'

    @constant
    def DB_HOST():
        # DbConnection::__init__
        # OwlCleanerCron.py
        return 'development.cgfo05y38ueo.us-east-1.rds.amazonaws.com'

    @constant
    def DB_USER():
        # DbConnection::__init__
        # OwlCleanerCron.py
        return 'devmaster'

    @constant
    def DB_PASS():
        # DbConnection::__init__
        # OwlCleanerCron.py
        return 'd*t*1sb3*^t1f^L'

    @constant
    def DB_DEMO_HOST():
        # OwlCleanerCron.py
        return 'demo-db.cgfo05y38ueo.us-east-1.rds.amazonaws.com'

    @constant
    def DB_DEMO_USER():
        # OwlCleanerCron.py
        return 'demomaster'

    @constant
    def DB_DEMO_PASS():
        # OwlCleanerCron.py
        return '2L1ttL32L1ttl3Z'

    @constant
    def RABBIT_MANAGER_HOST():
        #WorkerMonitor::__init__
        #ProcManager::__init__
        #rabbit_monitor.py
        return 'localhost:15672'

    @constant
    def RABBIT_MANAGER_PASS():
        #WorkerMonitor::__init__
        #ProcManager::__init__
        #rabbit_monitor.py
        ''' needs to change from dev->prod and from caverave'''
        return '02725700632541d48b439aadad7cd97c'

    @constant
    def RABBIT_MANAGER_USER():
        #WorkerMonitor::__init__
        #ProcManager::__init__
        #rabbit_monitor.py
        return 'chkm8-manager'

    @constant
    def RABBIT_WORKER_HOST():
        #RabbitMQWorker::main
        return 'localhost'

    @constant
    def RABBIT_WORKER_PORT():
        #RabbitMQWorker::main
        return 5672

    @constant
    def HIPCHAT_WORKERS_ROOM_ID():
        #WorkerMonitor::post_to_hipchat
        #ProcManager::post_to_hipchat
        #start-rabbit.sh
        #start-manager.sh
        #RabbitMQWorker::post_to_hipchat
        return '518943'

    @constant
    def HIPCHAT_CREDIT_CARD_ROOM_ID():
        #AuthLib::report_error
        return '518943'

    @constant
    def HIP_CHAT_ROOM_API():
        #Issues:Dev
        return '792959'

    @constant
    def HIPCHAT_AUTH_TOKEN():
        #WorkerMonitor::post_to_hipchat
        #ProcManager::post_to_hipchat
        #start-rabbit.sh
        #start-manager.sh
        #RabbitMQWorker::post_to_hipchat
        #AuthLib::report_error
        return '987ce42cc059f3a404e18c9f9c9642'

    @constant
    def REDIS_ORDERS_HOST():
        return '10.0.0.97'

    @constant
    def REDIS_ORDERS_PORT():
        return 6379

    @constant
    def REDIS_ORDERS_DB():
        return 0

    @constant
    def REDIS_ORDERS_PASSWORD():
        return 'fa0231a898bc62cd55f554191f219ecd829159c0cfa980f91e77b5bc82059f08b7ebe92b3d11946444c5ce360dfcf833d72948902566d3f64b8e7f1d7313e8243276fc1f6d5b9117098c7bb6d62a67873f9132ab27d1291b9c696273e7ed6e39292eb2ebbbab84e6ef6efaaf73b6e111d6df133646e8684cc2835b2874b1ddb7'

    @constant
    def REDIS_CACHE_HOST():
        #parse_preorder.py - def bustPreorderCache
        return '52.2.168.197'

    @constant
    def REDIS_CACHE_PORT():
        #parse_preorder.py - def bustPreorderCache
        ''' DOES DEV HAVE CACHE? '''
        return 6379

    @constant
    def REDIS_CACHE_DB():
        #parse_preorder.py - def bustPreorderCache
        return 1

    @constant
    def REDIS_CACHE_PASSWORD():
        #parse_preorder.py - def bustPreorderCache
        return 'b1c5dbee8b9eb34eae5deee2064694864188bfe1e55aba36a76a18a4b9eb0f66fad1eeafb121746857dc83911dacd953aa28e6dbe1b08cd2249c6f093f7acb0207449733e3188cbe7edc4319b2806afe0e59bad6b17583d287ffe2371891326c620ec575f70c34e57e85561aea38d2318f2d605b624d847ab282583725dc8ab5'

    @constant
    def REDIS_PAYMENT_BINS_HOST():
        return '52.2.168.197'

    @constant
    def REDIS_PAYMENT_BINS_PORT():
        return 6368

    @constant
    def REDIS_PAYMENT_BINS_DB():
        return 0

    @constant
    def REDIS_PAYMENT_BINS_PASSWORD():
        return 'e34df87619b9d29cec132f0edc55d883923e9eac13374606324e01a62ce8c0614c19393c083d106d751adec512d20df6899aa5db290b3d5ab5c032a2a8ad643ed8918d5841aa7f6a201c8318b8a5fbe2e3394fa4b57d04b63ee9504dc45b3742cce102d02793a7b39fb5b064c171858701beb70355ff9b749cf26750787d7599'

    @constant
    def REDIS_SOCIAL_PUBSUB_HOST():
        #twitter_cron.py
        #WeatherDb::updateCachedLocationWeatherCondition
        #SocialDb::updateCachedSocialData
        return '52.2.168.197'

    @constant
    def REDIS_SOCIAL_PUBSUB_PORT():
        #twitter_cron.py
        #WeatherDb::updateCachedLocationWeatherCondition
        #SocialDb::updateCachedSocialData
        return 6378

    @constant
    def REDIS_SOCIAL_PUBSUB_DB():
        #twitter_cron.py        
        #WeatherDb::updateCachedLocationWeatherCondition        
        #SocialDb::updateCachedSocialData
        return 0

    @constant
    def REDIS_SOCIAL_PUBSUB_PASSWORD():
        #twitter_cron.py
        #WeatherDb::updateCachedLocationWeatherCondition
        #SocialDb::updateCachedSocialData
        return 'a3c659a49fdf1251a43b127131db3762034008247b5507e382f5a0654a7993c9a3935f1f1e2d545f3e34cd8ddd7b6b6cd215b4312aab8639d205890f7518f4e54cadc7e437885ec431477262a4dca00e3fae81bb87558228ec9b2082658abd9951ec7ecfcbe18b3df5bb1bcd60b759431dced62f9457acc38b67edc50a456378'

    @constant
    def SUPPORT_EMAIL():
        #parse_preorder.py - parse_preorder
        # return ''
        #return jonathan@parametricdining.com'
        return 'wyatt@parametricdining.com'

    @constant
    def LOGPATH():
        #RabbitMQWorker::setup_logging
        #logfile_cleanup.py
        return '/data/serverlogs/rabbitmq_workers_logs'

    @constant
    def FORT_KNOX_URL():
        # KeyMaster::__init__
        return 'http://10.0.1.112/index.php'

    @constant
    def SALT_PATRON_CARDS():
        # PreauthDb::getPatronAndCardByCC
        return '8056ae8af38dfe1daaf023284519b252'

    @constant
    def SALT_PATRON_EMAIL():
        # ??
        return '2bf7a577975f66e81db23422fe5449dd'

    @constant
    def SALT_PATRONS():
        # preorder_db.py - PreorderDb
        return '19eb77279c640d05a610fae247c4934f'

    @constant
    def SALT_MERCHANTS():
        # preorder_db.py - PreorderDb
        return '442b5379fca08fc32adb0447d57c1458'

    @constant
    def PORTAL_URL():
        return 'https://fiene.paywithcheckmate.com';

    @constant
    def LEVY_LOG_SECRET_KEY():
        return '736d8606201dba5afc768b629662b6ec04e3ef07b895d8f23f057515cd000ccc'

    @constant
    def LEVY_LOG_PARTNER_ID():
        return 'parametric_python_worker'


    @constant
    def MAILGUN_API_KEY():
        return 'key-65618c6ea2e9cb5c108a81976d25663e'
    
    @constant
    def MAILGUN_DEV_DOMAIN():
        return 'sandbox70ee9d4680d44f4aa4fffd571dd88d8b.mailgun.org'

    @constant
    def MAILGUN_PROD_RECEIPT_DOMAIN():
        return 'receipts.bypassmobile.com'

    @constant
    def MAILGUN_PROD_GENERAL_DOMAIN():
        return 'mail.bypassmobile.com'

    @constant 
    def BATCH_REPORT_LOG_PATH():
        return '/var/log/batch_reports/BatchReporting-{0}.log'

    '''
    if __name__ == "__main__":
        print "winning!"
        cmc = CheckMateConfig()
        print "utg port = {0}".format(cmc.UTG_PORT)
        cmc.UTG_PORT = 9000
        print "end"
    '''
