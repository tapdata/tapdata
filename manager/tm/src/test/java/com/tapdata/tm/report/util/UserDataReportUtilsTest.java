package com.tapdata.tm.report.util;

import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Random;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserDataReportUtilsTest {
    @Nested
    class GenerateMachineIdTest{
        @Test
        @SneakyThrows
        @DisplayName("test generateMachineId method normal")
        void test1(){
            NetworkInterface networkInterface = mock(NetworkInterface.class);
            Vector v = new Vector();
            v.addElement(networkInterface);
            Enumeration<NetworkInterface> interfaces = v.elements();
            byte[] mac = new byte[1];
            mac[0] = 1;
            try (MockedStatic<NetworkInterface> mb = Mockito
                    .mockStatic(NetworkInterface.class)) {
                mb.when(NetworkInterface::getNetworkInterfaces).thenReturn(interfaces);
                when(networkInterface.getHardwareAddress()).thenReturn(mac);
                String actual = UserDataReportUtils.generateMachineId();
                assertEquals("[01]", actual);
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("test generateMachineId method when mac is null")
        void test2(){
            NetworkInterface networkInterface = mock(NetworkInterface.class);
            Vector v = new Vector();
            v.addElement(networkInterface);
            Enumeration<NetworkInterface> interfaces = v.elements();
            try (MockedStatic<NetworkInterface> mb = Mockito
                    .mockStatic(NetworkInterface.class)) {
                mb.when(NetworkInterface::getNetworkInterfaces).thenReturn(interfaces);
                when(networkInterface.getHardwareAddress()).thenReturn(null);
                String actual = UserDataReportUtils.generateMachineId();
                assertEquals("[]", actual);
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("test generateMachineId method with exception")
        void test3(){
            NetworkInterface networkInterface = mock(NetworkInterface.class);
            Vector v = new Vector();
            v.addElement(networkInterface);
            try (MockedStatic<NetworkInterface> mb = Mockito
                    .mockStatic(NetworkInterface.class)) {
                mb.when(NetworkInterface::getNetworkInterfaces).thenThrow(SocketException.class);
                when(networkInterface.getHardwareAddress()).thenReturn(null);
                String actual = UserDataReportUtils.generateMachineId();
                assertEquals("[]", actual);
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("test generateMachineId method when mac address exceed param max length")
        void test4(){
            NetworkInterface networkInterface = mock(NetworkInterface.class);
            Vector v = new Vector();
            v.addElement(networkInterface);
            Enumeration<NetworkInterface> interfaces = v.elements();
            byte[] mac = new byte[50];
            for (int i=0;i<50;i++){
                mac[i] = 1;
            }
            try (MockedStatic<NetworkInterface> mb = Mockito
                    .mockStatic(NetworkInterface.class)) {
                mb.when(NetworkInterface::getNetworkInterfaces).thenReturn(interfaces);
                when(networkInterface.getHardwareAddress()).thenReturn(mac);
                String actual = UserDataReportUtils.generateMachineId();
                assertEquals(100, actual.length());
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("test generateMachineId method when mac address less than param max length")
        void test5(){
            NetworkInterface networkInterface = mock(NetworkInterface.class);
            Vector v = new Vector();
            v.addElement(networkInterface);
            Enumeration<NetworkInterface> interfaces = v.elements();
            byte[] mac = new byte[20];
            for (int i=0;i<20;i++){
                mac[i] = 1;
            }
            try (MockedStatic<NetworkInterface> mb = Mockito
                    .mockStatic(NetworkInterface.class)) {
                mb.when(NetworkInterface::getNetworkInterfaces).thenReturn(interfaces);
                when(networkInterface.getHardwareAddress()).thenReturn(mac);
                String actual = UserDataReportUtils.generateMachineId();
                assertNotEquals(100, actual.length());
            }
        }
    }
}
