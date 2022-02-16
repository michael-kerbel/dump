package util.dump.externalization;

import java.io.Serializable;
import java.util.Random;


public class DumpOfferBeanSerializable implements Serializable {

   private static final long   RANDOM_SEED      = 123456;
   private static final Random RANDOM           = new Random(RANDOM_SEED);

   private static final long   serialVersionUID = 432200534464637912L;


   public static DumpOfferBeanSerializable createRandomBean() {
      DumpOfferBeanSerializable d = new DumpOfferBeanSerializable();
      d.code = randomString();
      d.codeType = RANDOM.nextInt();
      d.contentId = RANDOM.nextLong();
      d.created = RANDOM.nextInt();
      d.deeplink = randomString();
      d.deliveryCost = randomString();
      d.deliveryCostValue = RANDOM.nextDouble();
      d.deliveryTime = randomString();
      d.deliveryTimeColor = RANDOM.nextInt();
      d.description = randomString();
      d.hid = RANDOM.nextLong();
      d.imageSmall = randomString();
      d.imageView = randomString();
      d.isActive = RANDOM.nextInt() % 2 == 0 ? true : false;
      d.manufacturer = randomString();
      d.manufacturerPrdId = randomString();
      d.matchOwner = RANDOM.nextLong();
      d.matchProgram = RANDOM.nextLong();
      d.matchStamp = RANDOM.nextLong();
      d.matchType = RANDOM.nextLong();
      d.name = randomString();
      d.oldPrice = RANDOM.nextDouble();
      d.ownerId = RANDOM.nextLong();
      d.parentHid = RANDOM.nextLong();
      d.price = RANDOM.nextDouble();
      d.promoText = randomString();
      d.sourceGroup = randomString();
      d.sourceId = randomString();
      d.updated = RANDOM.nextLong();
      d.variant = RANDOM.nextLong();
      return d;
   }

   private static String randomString() {
      int length = RANDOM.nextInt(50);
      StringBuilder sb = new StringBuilder(length);
      for ( int i = 0; i < length; i++ ) {
         sb.append((char)(RANDOM.nextInt(64) + 32));
      }
      return sb.toString();
   }

   public long    contentId;
   public long    ownerId;
   public String  sourceId;
   public String  sourceGroup;

   public boolean isActive;

   public long    created;
   public long    updated;

   public long    hid;
   public long    parentHid;
   public long    variant;

   public long    matchOwner;
   public long    matchProgram;
   public long    matchType;
   public long    matchStamp;

   public String  name;
   public String  description;
   public String  manufacturer;
   public String  manufacturerPrdId;
   public String  promoText;

   public double  price;
   public Double  oldPrice;

   public String  code;
   public int     codeType;

   public String  deeplink;

   public String  deliveryTime;
   public int     deliveryTimeColor;
   public String  deliveryCost;
   public Double  deliveryCostValue;

   public String  imageView;
   public String  imageSmall;


   public DumpOfferBeanSerializable() {}

   public int compareTo( DumpOfferBeanSerializable o ) {
      return this.contentId < o.contentId ? -1 : (this.contentId == o.contentId ? 0 : 1);
   }

   @Override
   public boolean equals( Object obj ) {
      if ( this == obj ) {
         return true;
      }
      if ( obj == null ) {
         return false;
      }
      if ( getClass() != obj.getClass() ) {
         return false;
      }
      DumpOfferBeanSerializable other = (DumpOfferBeanSerializable)obj;
      if ( code == null ) {
         if ( other.code != null ) {
            return false;
         }
      }
      else if ( !code.equals(other.code) ) {
         return false;
      }
      if ( codeType != other.codeType ) {
         return false;
      }
      if ( contentId != other.contentId ) {
         return false;
      }
      if ( created != other.created ) {
         return false;
      }
      if ( deeplink == null ) {
         if ( other.deeplink != null ) {
            return false;
         }
      }
      else if ( !deeplink.equals(other.deeplink) ) {
         return false;
      }
      if ( deliveryCost == null ) {
         if ( other.deliveryCost != null ) {
            return false;
         }
      }
      else if ( !deliveryCost.equals(other.deliveryCost) ) {
         return false;
      }
      if ( deliveryCostValue == null ) {
         if ( other.deliveryCostValue != null ) {
            return false;
         }
      }
      else if ( !deliveryCostValue.equals(other.deliveryCostValue) ) {
         return false;
      }
      if ( deliveryTime == null ) {
         if ( other.deliveryTime != null ) {
            return false;
         }
      }
      else if ( !deliveryTime.equals(other.deliveryTime) ) {
         return false;
      }
      if ( deliveryTimeColor != other.deliveryTimeColor ) {
         return false;
      }
      if ( description == null ) {
         if ( other.description != null ) {
            return false;
         }
      }
      else if ( !description.equals(other.description) ) {
         return false;
      }
      if ( hid != other.hid ) {
         return false;
      }
      if ( imageSmall == null ) {
         if ( other.imageSmall != null ) {
            return false;
         }
      }
      else if ( !imageSmall.equals(other.imageSmall) ) {
         return false;
      }
      if ( imageView == null ) {
         if ( other.imageView != null ) {
            return false;
         }
      }
      else if ( !imageView.equals(other.imageView) ) {
         return false;
      }
      if ( isActive != other.isActive ) {
         return false;
      }
      if ( manufacturer == null ) {
         if ( other.manufacturer != null ) {
            return false;
         }
      }
      else if ( !manufacturer.equals(other.manufacturer) ) {
         return false;
      }
      if ( manufacturerPrdId == null ) {
         if ( other.manufacturerPrdId != null ) {
            return false;
         }
      }
      else if ( !manufacturerPrdId.equals(other.manufacturerPrdId) ) {
         return false;
      }
      if ( matchOwner != other.matchOwner ) {
         return false;
      }
      if ( matchProgram != other.matchProgram ) {
         return false;
      }
      if ( matchStamp != other.matchStamp ) {
         return false;
      }
      if ( matchType != other.matchType ) {
         return false;
      }
      if ( name == null ) {
         if ( other.name != null ) {
            return false;
         }
      }
      else if ( !name.equals(other.name) ) {
         return false;
      }
      if ( oldPrice == null ) {
         if ( other.oldPrice != null ) {
            return false;
         }
      }
      else if ( !oldPrice.equals(other.oldPrice) ) {
         return false;
      }
      if ( ownerId != other.ownerId ) {
         return false;
      }
      if ( parentHid != other.parentHid ) {
         return false;
      }
      if ( Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price) ) {
         return false;
      }
      if ( promoText == null ) {
         if ( other.promoText != null ) {
            return false;
         }
      }
      else if ( !promoText.equals(other.promoText) ) {
         return false;
      }
      if ( sourceGroup == null ) {
         if ( other.sourceGroup != null ) {
            return false;
         }
      }
      else if ( !sourceGroup.equals(other.sourceGroup) ) {
         return false;
      }
      if ( sourceId == null ) {
         if ( other.sourceId != null ) {
            return false;
         }
      }
      else if ( !sourceId.equals(other.sourceId) ) {
         return false;
      }
      if ( updated != other.updated ) {
         return false;
      }
      if ( variant != other.variant ) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      throw new UnsupportedOperationException("DumpOfferBeanSerializable does not implement hashCode, don't put it into HashMaps");
   }
}
