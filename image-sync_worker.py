#!/usr/bin/env python
from rabbitmq_worker import RabbitMQWorker
import logging
import zipfile
from media_db import MediaDb
import os
import traceback
import json
import media_db

class ImageSyncWorker(RabbitMQWorker):
    def sync_images(self, msg_dict, db, logger_name):
        success = True
        log_man = logging.getLogger(logger_name)
        if "venueUid" not in msg_dict:
            raise Exception("Missing valid venue_uid")
        if "hashOfHashes" not in msg_dict:
            raise Exception("Missing required hash")
        if "newImages" not in msg_dict:
            raise Exception("Missing required new images")
        if "syncActionUid" not in msg_dict:
            raise Exception("Missing required sync_action_uid")
        venue_uid = int( msg_dict['venueUid'] )
        hash_of_hashes = str( msg_dict['hashOfHashes'] )
        new_images = msg_dict['newImages']
        tablet_image_sync_action_uid = int( msg_dict['syncActionUid'] )
        bundle_file = '/data/media/{0}/images/bundles/{1}.zip'.format(venue_uid, hash_of_hashes)
        if os.path.isfile(bundle_file):
            log_man.debug(" [>] Bundle File Already Exists: {0}".format(bundle_file))
        else:
            zipf = zipfile.ZipFile(bundle_file, 'w', zipfile.ZIP_DEFLATED)
            try:
                for image_type in new_images:
                    image_type = str( image_type )
                    path = '/data/media/{0}/images/{1}/'.format(venue_uid, image_type)
                    for image_data in new_images[image_type]:
                        pointer_uid = str( image_data['pointer_uid'] )
                        img_file = path + pointer_uid + '.png'
                        if os.path.isfile(img_file):
                            log_man.debug(" [>] Zipping File = {0}".format(img_file))
                            zipf.write(img_file)
                        else:
                            img_file = path + pointer_uid + '.jpg'
                            if os.path.isfile(img_file):
                                log_man.debug(" [>] Zipping File = {0}".format(img_file))
                                zipf.write(img_file)

            except Exception, e:
                #Close the Zipfile and Re-raise the exception
                zipf.close()
                success = False
                raise
            zipf.close()
        return success

    def worker_function(self, ch, msg_dict, db, logger_name, worker_class):
        log_man = logging.getLogger(logger_name)
        log_man.debug(" [>] Received {0}".format(msg_dict))
        result_msg_list = list()
        report_error = False
        success = False
        db_success = False
        e = ''
        media_db = MediaDb(db)
        try:
            success = self.sync_images(msg_dict, media_db, logger_name)
        except Exception:
            report_error = True
            trace_str = str(traceback.format_exc())
            error_str = 'Error Raised in Sync Images - Trace: {0}'.format(trace_str)
            result_msg_list.append(error_str)
            log_man.debug(error_str)
        try:
            sync_action_uid = int( msg_dict['syncActionUid'] )
            status = 'done' if success else 'fail'
            db_success = media_db.updateTabletImageSyncAction(sync_action_uid, status)
        except Exception:
            report_error = True
            trace_str = str(traceback.format_exc())
            error_str = 'Error Raised while updating the database with bundle status: Trace: {0}'.format(trace_str)
            result_msg_list.append(error_str)
            log_man.debug(error_str)

        if not db_success:
            report_error = True
            error_str = 'Failure to properly update the database in Image Sync'
            result_msg_list.append(error_str)
            log_man.debug( error_str )
            success = False
        log_man.debug( " [<] DB Update Status %s".format(success))
        if report_error:
            result_msg_list.insert(0, 'Image Sync Failure - Input Msg: {0}'.format(json.dumps(msg_dict)))
            result_msg = ' || '.join(result_msg_list)
            self.post_to_hipchat(result_msg, color='red', notify=True)
        return success

if __name__ == "__main__":
    rabbit_worker = ImageSyncWorker()
    rabbit_worker.start()
