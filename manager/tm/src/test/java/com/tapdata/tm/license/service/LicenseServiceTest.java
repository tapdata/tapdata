package com.tapdata.tm.license.service;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

//@SpringBootTest
//@RunWith(SpringRunner.class)
class LicenseServiceTest {

    @Autowired
    LicenseService licenseService;

    @Test
    public void decryptLicense() throws Throwable {

        String license = "qchOrRwytx0HnD7+willJk0yzltLFuxC1y9S6c/BszqF6Q9Ne8Xfq9YmplHurjtdTEIon0+pB9DqtkCMp4IUizQnFsc1hvXocExCRMcYwu18bCPL4j4ZdOULc/onad8U2PqNTrLJSpCypUODde5HP6YdQGvf+7Lo+n6jevAnsE4KCUZ98+XgwGkoqOl4vZHhyW/m5cpWd3wj22RfbZ0p8IUJBdU2+OE0bv8UniF5JWmrXBI3m79iELGEAQDl5MKqEscgeJaT6C0DQerlgYC+Bw==.Q8AFYViLwnM0iCbqb8tN+9RkUehi+HbbChtICjcCPdr1X2qR8aVwK0RuQqOv1Oe0DDBNZ2mCbFnyESze3l0pIbATS7L6l76FT+3gIBrm19DeNDerFCIT0VQvGoQT89bdlDehtsTGdnv92ogXL1gaQO9VzTmevpGGaZbxVn38SNPvpZwXnvjAw47iqvgMVJpPJJpfASrXcwUvr+PHkPuipXvcLZkfe9dQd47L+IgPuQcYgI59E1BgG3ju1QrT83fRBiTMNVYJ14P6ZKjBazBmRtraIba9JKMEl+Nu5pLX+3Qb9KKBK3kcAuqTyqyVT3TlCA/lr5Msc319efOC/dMBtA==";
        String s = licenseService.decryptLicense(license);
        System.out.println(s);
    }

    @Test
    public void test() {
        long time = new Date().getTime();
        System.out.println(time);
        System.out.println(String.valueOf(time));
        System.out.println();
    }

}