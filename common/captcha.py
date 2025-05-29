import ddddocr 
from settings import CAPTCHA,CAPTCHA_IMAGE_PATH

class CaptchaHandler:
    """
    验证码处理类
    """
    def __init__(self):
        """
        初始化 CaptchaHandler 实例
        """
        # show_ad=False 参数表示在识别过程中不显示广告信息
        self.ocr = ddddocr.DdddOcr(show_ad=False)
        self.captcha_type = CAPTCHA

    def parse_image_captcha(self, image_path):
        """解析图片验证码"""
        try:
            with open(image_path, 'rb') as f:
                code = f.read()
                return self.ocr.classification(code)
        except Exception as e:
            raise ValueError('验证码解析失败') from e


    def get_test_captcha(self):
        """获取测试环境专用验证码（需与开发约定）"""
        return self.captcha_type

# if __name__ == '__main__':
#     print(CaptchaHandler().parse_image_captcha(CAPTCHA_IMAGE_PATH))

